package raft

func (n *Node) Propose(data string) (int, uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.storageErr != nil || n.role != Leader {
		return -1, n.term, false
	}
	n.log = append(n.log, LogEntry{Term: n.term, Data: data})
	if err := n.persist(); err != nil {
		return -1, n.term, false
	}
	index := n.lastIndex()
	n.matchIndex[n.id] = index
	n.nextIndex[n.id] = index + 1
	n.advanceCommit()
	n.broadcast()
	return index, n.term, true
}

func (n *Node) advanceCommit() {
	for index := n.lastIndex(); index > n.commitIndex; index-- {
		if n.termAt(index) != n.term {
			continue
		}
		votes := 0
		for _, match := range n.matchIndex {
			if match >= index {
				votes++
			}
		}
		if votes > len(n.peers)/2 {
			n.commitIndex = index
			n.notifyApply()
			return
		}
	}
}
