package consts

const (
	TaskStatusIdle			= 	0
	TaskStatusRunning		=	1
	TaskStatusFailed		=	2
	TaskStatusFinished		=	3

	WorkerStatusIdle	=	0
	WorkerStatusRunging	=	1
	WorkerStatusFailed	=	2

	MethodAcquireTask	= "Coordinator.AcquireTask"
	MethodFinished		= "Coordinator.Finished"
)
