<p align="center">
   <img src="https://github.com/netrusov/easyraft/raw/main/logo.png" width="50%">
</p>

## HTTP based key-value store example on Kubernetes (webkvs)

This example demonstrates, how easily you can implement an HTTP based in-memory
distributed key value store using EasyRaft running on Kubernetes

## Usage

1. Run example:
   1. `docker compose up easyraft`
1. Exec into `bind-tools` container
   1. `docker compose run --rm bind-tools sh`
1. Obtain node IPs:
   1. `dig +short easyraft`
1. Put value on one of the nodes:
   1. `curl --location --request POST 'http://172.19.0.2:5002/put?map=test&key=somekey&value=somevalue'`
1. Get value:
   1. `curl --location --request GET 'http://172.19.0.2:5002/get?map=test&key=somekey'`
   1. `curl --location --request GET 'http://172.19.0.3:5002/get?map=test&key=somekey'`
   1. `curl --location --request GET 'http://172.19.0.4:5002/get?map=test&key=somekey'`
1. Start more replicas by adjusting `scale` attribute and see that new nodes joined to cluster successfully
