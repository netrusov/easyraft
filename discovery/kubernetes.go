package discovery

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	defaultK8sDiscoveryNodePortName = "easyraft"
	defaultK8sDiscoverySvcType      = "easyraft"
)

type KubernetesDiscovery struct {
	namespace             string
	matchingServiceLabels map[string]string
	nodePortName          string
	delayTime             time.Duration

	mu          sync.Mutex
	discoveryCh chan string
	done        chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

func NewKubernetesDiscovery(namespace string, serviceLabels map[string]string, raftPortName string) DiscoveryMethod {
	if raftPortName == "" {
		raftPortName = defaultK8sDiscoveryNodePortName
	}

	if serviceLabels == nil || len(serviceLabels) == 0 {
		serviceLabels = map[string]string{"svcType": defaultK8sDiscoverySvcType}
	}

	delayTime := time.Duration(rand.Intn(5)+1) * time.Second

	return &KubernetesDiscovery{
		namespace:             namespace,
		matchingServiceLabels: serviceLabels,
		nodePortName:          raftPortName,
		delayTime:             delayTime,
	}
}

func (k *KubernetesDiscovery) Start(_ string, _ int) (<-chan string, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	out := make(chan string)
	done := make(chan struct{})

	k.mu.Lock()
	k.discoveryCh = out
	k.done = done
	k.stopOnce = sync.Once{}
	k.mu.Unlock()

	k.wg.Add(1)
	go k.discovery(clientSet, out, done)

	return out, nil
}

func (k *KubernetesDiscovery) discovery(clientSet *kubernetes.Clientset, out chan string, done <-chan struct{}) {
	defer k.wg.Done()
	defer close(out)

	ticker := time.NewTicker(k.delayTime)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		default:
		}

		services, err := clientSet.CoreV1().Services(k.namespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: labels.SelectorFromSet(k.matchingServiceLabels).String(),
			Watch:         false,
		})
		if err != nil {
			log.Println(err)
		} else {
			for _, svc := range services.Items {
				set := labels.Set(svc.Spec.Selector)
				listOptions := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(set).String()}

				pods, err := clientSet.CoreV1().Pods(svc.Namespace).List(context.Background(), listOptions)
				if err != nil {
					log.Println(err)
					continue
				}

				for _, pod := range pods.Items {
					if strings.ToLower(string(pod.Status.Phase)) != "running" {
						continue
					}

					podIP := pod.Status.PodIP
					var raftPort v1.ContainerPort
					for _, container := range pod.Spec.Containers {
						for _, port := range container.Ports {
							if port.Name == k.nodePortName {
								raftPort = port
								break
							}
						}
					}

					if podIP == "" || raftPort.ContainerPort == 0 {
						continue
					}

					select {
					case out <- fmt.Sprintf("%v:%v", podIP, raftPort.ContainerPort):
					case <-done:
						return
					}
				}
			}
		}

		select {
		case <-done:
			return
		case <-ticker.C:
		}
	}
}

func (k *KubernetesDiscovery) SupportsNodeAutoRemoval() bool {
	return true
}

func (k *KubernetesDiscovery) Stop() error {
	k.mu.Lock()
	done := k.done
	k.mu.Unlock()

	if done == nil {
		return nil
	}

	k.stopOnce.Do(func() {
		close(done)
	})
	k.wg.Wait()

	k.mu.Lock()
	k.discoveryCh = nil
	k.done = nil
	k.mu.Unlock()

	return nil
}
