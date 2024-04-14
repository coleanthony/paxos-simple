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
	prepareOK     []bool
	//acceptOK                   []bool
	PeersMaxAcceptedProposeNum []int
	PeersMaxAcceptedProposeVal []interface{}

	//acceptor
	accpetedvalues     interface{} //accpeted value
	highestPrepareSeen int
	highestAcceptSeen  int

	//learner
	decidedvalues interface{} //decided value
}

func makeInstance(seqNum int, value interface{}, numpeers int) *Instance {
	//fmt.Println(numpeers)
	instance := &Instance{
		Proposing:   false,
		sequenceNum: seqNum,

		proposevalues: value,
		proposeNum:    -1,
		prepareOKNum:  0,
		acceptOKNum:   0,
		prepareOK:     make([]bool, numpeers),
		//acceptOK:                   make([]bool, numpeers),
		PeersMaxAcceptedProposeNum: make([]int, numpeers),
		PeersMaxAcceptedProposeVal: make([]interface{}, numpeers),

		accpetedvalues:     nil,
		highestPrepareSeen: -1,
		highestAcceptSeen:  -1,

		decidedvalues: nil,
	}
	for i := 0; i < numpeers; i++ {
		instance.PeersMaxAcceptedProposeNum[i] = -1
		instance.PeersMaxAcceptedProposeVal = nil
		instance.prepareOK[i] = false
		//instance.acceptOK[i] = false
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

//used when propose timeout, need to reset the instance
func (px *Paxos) resetInstance(ins *Instance, value interface{}) {
	ins.proposevalues = value
	ins.prepareOKNum = 0
	ins.acceptOKNum = 0
	for i := 0; i < len(px.peers); i++ {
		ins.PeersMaxAcceptedProposeNum[i] = -1
		ins.PeersMaxAcceptedProposeVal = nil
		ins.prepareOK[i] = false
		//ins.acceptOK[i] = false
	}
}
