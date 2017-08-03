package master

type WorkerCtl interface {
	InspectWorker(workernum int) string
	StartWorkers(num int) error
	StopWorkers()
	GetWorkerNum() int
}

type WorkerCtlBase struct {
}

func (wcb *WorkerCtlBase) InspectWorkers(workernums ...int) map[int]string {
	return nil
}
