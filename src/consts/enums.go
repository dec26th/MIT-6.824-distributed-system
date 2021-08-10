package consts


type TaskType int8
type CoordinatorType int8
const (
	TaskStatusIdle			= 	0
	TaskStatusRunning		=	1
	TaskStatusFinished		=	2

	MethodAcquireTask	= "Coordinator.AcquireTask"
	MethodFinished		= "Coordinator.Finished"

	TaskTypeMap			= TaskType(0)
	TaskTypeReduce		= TaskType(1)

	CoordinatorTypeHasTasks = CoordinatorType(0)
	CoordinatorTypeNoTask   = CoordinatorType(1)
)
