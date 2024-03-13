package paxos

import (
	"fmt"
	"sync"
)

func (px *Paxos) broadcastAccept(ins *Instance, acceptedok chan bool) {
	px.mu.Lock()
	sequencenum := ins.sequenceNum
	proposenum := ins.proposeNum
	proposeval := ins.proposevalues
	px.mu.Unlock()

	var wg sync.WaitGroup
	for i := range px.peers {
		if i == px.me {
			args := &AcceptArgs{
				Me:         px.me,
				Value:      proposeval,
				ProposeNum: proposenum,
				SeqNum:     sequencenum,
			}
			reply := &AcceptReplys{}
			px.Accept(args, reply)
			px.AcceptHandler(args, reply)
			continue
		}
		wg.Add(1)
		go func(i int) {
			//send accept to peer i
			defer wg.Done()
			args := &AcceptArgs{
				Me:         px.me,
				Value:      proposeval,
				ProposeNum: proposenum,
				SeqNum:     sequencenum,
			}
			reply := &AcceptReplys{}
			ok := call(px.peers[i], "Paxos.Accept", args, reply)
			if ok {
				px.AcceptHandler(args, reply)
			} else {
				fmt.Printf("proposer %d send proposed value to acceptor %d error\n", px.me, i)
			}
		}(i)
	}
	wg.Wait()

	px.mu.Lock()
	if ins.acceptOKNum >= px.Majority() {
		acceptedok <- true
	}
	px.mu.Unlock()
}

//acceptor: phase2b
//如果 round != last_round 说明存在冲突直接拒绝请求
//value 写入本地，应用于状态机
//将成功或失败的结果返回给proposer
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReplys) {
	px.mu.Lock()
	defer px.mu.Unlock()

	//do update
	px.UpdateMaxseenSeq(args.SeqNum)
	px.UpdateMaxseenProposeNum(args.ProposeNum)

	//reply args
	reply.Me = px.me
	reply.OK = true

	ins := px.getInstance(args.SeqNum)
	if args.ProposeNum < ins.highestPrepareSeen {
		reply.OK = false
		return
	}
	ins.highestAcceptSeen = args.ProposeNum
	ins.highestPrepareSeen = args.ProposeNum
	ins.accpetedvalues = args.Value
}

func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReplys) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if args.SeqNum <= px.maxForgottenSeqNum {
		return
	}

	ins := px.getInstance(args.SeqNum)
	if ins.decidedvalues != nil {
		return
	}
	if ins.proposeNum != args.ProposeNum {
		return
	}
	if reply.OK {
		ins.acceptOKNum++
	}

}
