package models

type FinishedReq struct {
	ID	int
}

type FinishedResp struct {}

type AcquireTaskReq struct {}

type AcquireTaskResp struct {
	Task		T
}

type T struct {
	ID       int
	FileName []string
	Status   int8
}
