package consts


type TaskType int8
type CoordinatorType int8

func (t TaskType) String() string {
	switch t {
	case TaskTypeMap:
		return "Map"
	case TaskTypeReduce:
		return "Reduce"
	}
	return "Unknown"
}

func (c CoordinatorType) String() string {
	switch c {
	case CoordinatorTypeHasTasks:
		return "There are tasks left"
	case CoordinatorTypeNoTask:
		return "There is no task"
	}
	return "Unknown"
}
const (
	TaskStatusIdle			= 	0
	TaskStatusRunning		=	1
	TaskStatusFinished		=	2

	MethodAcquireTask	= "Coordinator.AcquireTask"
	MethodFinished		= "Coordinator.Finished"
	MethodExit			= "Coordinator.Exit"

	TaskTypeMap			= TaskType(0)
	TaskTypeReduce		= TaskType(1)

	CoordinatorTypeHasTasks = CoordinatorType(0)
	CoordinatorTypeNoTask   = CoordinatorType(1)
)
