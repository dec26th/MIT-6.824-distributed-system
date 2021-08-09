package consts


type TaskType int8
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

	TaskTypeMap			= TaskType(0)
	TaskTypeReduce		= TaskType(1)
)
