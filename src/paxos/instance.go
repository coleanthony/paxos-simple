package paxos

type PeerState int

const (
	Proposer = iota
	Acceptor
	Learner
)

//proposer, acceptor, learner
type Instance struct {
	peerstate   PeerState
	sequenceNum int

	//proposer
	proposevalues interface{} //propose value
	proposeNum    int
	prepareOK     []bool
	acceptOK      []bool

	//acceptor
	accpetedvalues     interface{} //accpeted value
	highestPrepareSeen int
	highestAcceptSeen  int

	//learner
	decidedvalues interface{} //decided value
}
