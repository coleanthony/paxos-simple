package paxos

type PrepareArgs struct {
	Me         int
	ProposeNum int
}

type PrepareReplys struct {
	Me             int
	AcceptedNum    int
	AcceptedValues interface{}
	OK             bool
}

type AcceptArgs struct {
	Me         int
	Value      interface{}
	ProposeNum int
}

type AcceptReplys struct {
	Me int
	OK bool
}

type DecideArgs struct {
}

type DecideReplys struct {
}
