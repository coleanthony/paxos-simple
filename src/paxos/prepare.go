package paxos

import (
	"fmt"
	"sync"
	"time"
)

func (px *Paxos) broadcastPrepare(ins *Instance, prepareok chan bool) {
	px.mu.Lock()
	sequencenum := ins.sequenceNum
	proposenum := ins.proposeNum
	px.mu.Unlock()

	var wg sync.WaitGroup
	for i := range px.peers {
		if i == px.me {
			//is myself, do preparehandler
			args := &PrepareArgs{
				Me:         px.me,
				SeqNum:     sequencenum,
				ProposeNum: proposenum,
			}
			reply := &PrepareReplys{}
			px.Prepare(args, reply)
			px.PrepareHandler(args, reply)
			continue
		}
		wg.Add(1)
		//send prepare
		go func(i int) {
			//send prepare to peer i
			defer wg.Done()
			args := &PrepareArgs{
				Me:         px.me,
				SeqNum:     sequencenum,
				ProposeNum: proposenum,
			}
			reply := &PrepareReplys{}
			ok := call(px.peers[i], "Paxos.Prepare", args, reply)
			if ok {
				px.PrepareHandler(args, reply)
			} else {
				fmt.Printf("proposer %d send proposal to acceptor %d error\n", px.me, i)
			}
		}(i)
	}
	wg.Wait()

	px.mu.Lock()
	if ins.prepareOKNum >= px.Majority() {
		prepareok <- true
	}
	px.mu.Unlock()
}

//acceptor: phase1b
//如果接到的请求中round < 自身 last_round 则拒绝请求
//将自身的last_round = 请求中的round
//返回应答,带上自身的last_round, 上一个接受的value和value_round
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReplys) {
	px.mu.Lock()
	defer px.mu.Unlock()

	//do update
	px.UpdateMaxseenSeq(args.SeqNum)
	px.UpdateMaxseenProposeNum(args.ProposeNum)
	//reply args
	reply.Me = px.me
	reply.OK = true
	reply.AcceptedNum = -1
	reply.AcceptedValues = nil

	ins := px.getInstance(args.SeqNum)
	if args.ProposeNum < ins.highestPrepareSeen {
		//reject the proposal
		reply.OK = false
		return
	}
	ins.highestPrepareSeen = args.ProposeNum
	reply.AcceptedNum = ins.highestAcceptSeen
	reply.AcceptedValues = ins.accpetedvalues
}

//deal with prepare replys by proposer
func (px *Paxos) PrepareHandler(args *PrepareArgs, reply *PrepareReplys) {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.UpdateMaxseenProposeNum(args.ProposeNum)

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
		ins.prepareOKNum++
		ins.prepareOK[args.Me] = true
		ins.PeersMaxAcceptedProposeNum[args.Me] = reply.AcceptedNum
		ins.PeersMaxAcceptedProposeVal[args.Me] = reply.AcceptedValues
	}
}

func (px *Paxos) chooseProposeVal(ins *Instance) {
	px.mu.Lock()
	defer px.mu.Unlock()

	tmpMaxAcceptedProposeNum := -1
	for i := 0; i < len(px.peers); i++ {
		if ins.prepareOK[i] && ins.PeersMaxAcceptedProposeNum[i] < tmpMaxAcceptedProposeNum && ins.PeersMaxAcceptedProposeVal[i] != nil {
			tmpMaxAcceptedProposeNum = ins.PeersMaxAcceptedProposeNum[i]
			ins.proposevalues = ins.PeersMaxAcceptedProposeVal[i]
		}
	}
}

func (px *Paxos) chooseProposeNum() int {
	peernum := len(px.peers)
	proposenum := px.maxProposeNum
	for proposenum <= px.maxProposeNum {
		proposenum = px.me + px.roundNum*peernum
		px.roundNum++
	}
	return proposenum
}

//start a proposal
func (px *Paxos) Propose(seqNum int, value interface{}) {
	//propose a proposal, phase 1a and phase 1b

	for !px.isdead() {
		status, _ := px.Status(seqNum)
		if status != Pending {
			break
		}
		//phase1a
		px.mu.Lock()
		proposeNum := px.chooseProposeNum()
		ins := px.getInstance(seqNum)

		px.mu.Unlock()

		prepareok := make(chan bool)
		go px.broadcastPrepare(ins, prepareok)

		//返回的response中如果有last_round 大于round则直接退出
		//从所有response中选择value_round 最大的那个value
		//如果所有response中的value都是空，则选择自己提议的value
		//如果没有得到一个多数派集合的确认，则直接退出

		select {
		case majorityprepared := <-prepareok:
			if !majorityprepared {
				//prepare not ok
				fmt.Printf("propose phase1: majorities not prepared ok, sequencenum: %d , proposenum: %d \n", seqNum, proposeNum)
				continue
			}
			//prepare ok ,go to phase2a
		case <-time.After(proposeTimeout):
			//timeout
			fmt.Printf("phase1a timeout \n")
			continue
		}

		//phase2a
		//发送 phase-2 RPC，带上phase-1决定的value和本次的round
		px.chooseProposeVal(ins)
		acceptok := make(chan bool)
		go px.broadcastAccept(ins, acceptok)

		select {
		case majorityaccepted := <-acceptok:
			if !majorityaccepted {
				fmt.Printf("propose phase2: majorities not accepted ok, sequencenum: %d , proposenum: %d \n", seqNum, proposeNum)
				continue
			}
		case <-time.After(proposeTimeout):
			//timeout
			fmt.Printf("phase2a timeout \n")
			continue
		}

		//majority accepted,send decide to all
		px.mu.Lock()
		ins.decidedvalues = value
		ins.Proposing = false
		px.mu.Unlock()

		go px.broadcastDecide(ins)
		break
	}
}
