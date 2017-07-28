package kmr

type WorkerCtl interface {
	InspectWorker(workernum int) string
	StartWorkers(num int)
	StopWorkers()
	GetWorkerNum() int
}

type WorkerCtlBase struct {
	InspectWorkers(workernums ...int) map[int]string
}

func (wcb *WorkerCtlBase) InspectWorkers(workernums ...int) map[int]string {

}