package easyraft

var closedBoolCh <-chan bool = func() <-chan bool {
	ch := make(chan bool)
	close(ch)
	return ch
}()

func (n *Node) LeaderCh() <-chan bool {
	if n == nil || n.raft == nil {
		return closedBoolCh
	}
	return n.raft.LeaderCh()
}

func (n *Node) IsLeader() bool {
	if err := n.raft.VerifyLeader().Error(); err != nil {
		return false
	}

	return true
}
