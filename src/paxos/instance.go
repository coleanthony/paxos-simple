package paxos

type PeerState int

const (
	Proposer = iota
	Acceptor
	Learner
)

//proposer, acceptor, learner
type Instance struct {
	//peerstate   PeerState
	Proposing   bool //judge Fate
	sequenceNum int

	//proposer
	proposevalues interface{} //propose value
	proposeNum    int
	prepareOKNum  int
	acceptOKNum   int

	//acceptor
	accpetedvalues     interface{} //accpeted value
	highestPrepareSeen int
	highestAcceptSeen  int

	//learner
	decidedvalues interface{} //decided value
}

func makeInstance(seqNum int, value interface{}, numpeers int) *Instance {
	instance := &Instance{
		Proposing:   false,
		sequenceNum: seqNum,

		proposevalues: value,
		proposeNum:    -1,
		prepareOKNum:  0,
		acceptOKNum:   0,

		accpetedvalues:     nil,
		highestPrepareSeen: -1,
		highestAcceptSeen:  -1,

		decidedvalues: nil,
	}
	return instance
}

func (px *Paxos) getInstance(seqNum int) *Instance {
	ins, ok := px.instances[seqNum]
	if !ok {
		px.instances[seqNum] = makeInstance(seqNum, nil, len(px.peers))
		ins = px.instances[seqNum]
	}
	return ins
}

func (px *Paxos) setInstance(ins *Instance, value interface{}) {

}
