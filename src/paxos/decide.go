package paxos

import "fmt"

func (px *Paxos) broadcastDecide(ins *Instance) {
	px.mu.Lock()
	defer px.mu.Unlock()

	sequencenum := ins.sequenceNum
	proposenum := ins.proposeNum
	decidedval := ins.decidedvalues
	doneseqnum := px.doneSeqs[px.me]

	for i := range px.peers {
		if i == px.me {
			go func() {
				args := &DecideArgs{
					Me:         px.me,
					Value:      decidedval,
					SeqNum:     sequencenum,
					ProposeNum: proposenum,
					DoneSeqNum: doneseqnum,
				}
				reply := &DecideReplys{}
				px.Decide(args, reply)
				px.DecideHandler(args, reply)
			}()
			continue
		}
		go func(i int) {
			args := &DecideArgs{
				Me:         px.me,
				Value:      decidedval,
				SeqNum:     sequencenum,
				ProposeNum: proposenum,
				DoneSeqNum: doneseqnum,
			}
			reply := &DecideReplys{}
			ok := call(px.peers[i], "Paxos.Decide", args, reply)
			if ok {
				px.DecideHandler(args, reply)
			} else {
				fmt.Printf("proposer %d send Decided value to acceptor %d error\n", px.me, i)
			}
		}(i)
	}

}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReplys) {
	px.mu.Lock()
	defer px.mu.Unlock()

	//do update
	px.UpdateMaxseenSeq(args.SeqNum)
	px.UpdateMaxseenProposeNum(args.ProposeNum)
	px.UpdateMaxseenDoneseq(args.DoneSeqNum, args.Me)

	reply.OK = true
	reply.DoneSeqNum = px.doneSeqs[px.me]

	ins := px.getInstance(args.SeqNum)

	if ins.decidedvalues != nil {
		return
	}
	if args.ProposeNum < ins.highestPrepareSeen {
		reply.OK = false
		return
	}

	ins.decidedvalues = args.Value
	//ins.highestAcceptSeen = args.ProposeNum
	//ins.highestPrepareSeen = args.ProposeNum
}

func (px *Paxos) DecideHandler(args *DecideArgs, reply *DecideReplys) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if args.SeqNum <= px.maxForgottenSeqNum {
		return
	}
}
