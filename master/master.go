package master

import (
	"net"
	"os"
	"time"

	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/util/log"

	"github.com/naturali/kmr/jobgraph"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"k8s.io/client-go/kubernetes"
)

const (
	HeartBeatCodePulse    = 0
	HeartBeatCodeDead     = 1
	HeartBeatCodeFinished = 2

	HeartBeatTimeout = 20 * time.Second
)

// Master is a map-reduce controller. It stores the state for each task and other runtime progress statuses.
type Master struct {
	port       string // Master listening port, like ":50051"
	nextTaskID int

	heartbeat     map[int64]chan int // Heartbeat channel for each worker
	workerTaskMap map[int64]TaskDescription

	k8sclient *kubernetes.Clientset

	job       *jobgraph.Job
	scheduler Scheduler

	checkpointFile *os.File

	requestTaskFunc RequestFunction
	reportTaskFunc  ReportFunction
}

func (master *Master) TaskSucceeded(jobDesc *jobgraph.JobDescription) error {
	return nil
}
func (master *Master) TaskFailed(jobDesc *jobgraph.JobDescription) error {
	return nil
}
func (master *Master) MapReduceNodeSucceed(node *jobgraph.MapReduceNode) error {
	return nil
}
func (master *Master) MapReduceNodeFailed(node *jobgraph.MapReduceNode) error {
	return nil
}
func (master *Master) JobNodeSucceeded(node *jobgraph.JobNode) error {
	return nil
}
func (master *Master) JobNodeFailed(node *jobgraph.JobNode) error {
	return nil
}

// CheckHeartbeatForEachWorker
// CheckHeartbeat keeps checking the heartbeat of each worker. It is either DEAD, PULSE, FINISH or losing signal of
// heartbeat.
// If the task is DEAD (occur error while the worker is doing the task) or cannot detect heartbeat in time. Master
// will releases the task, so that another work can takeover
// Master will check the heartbeat every 5 seconds. If master cannot detect any heartbeat in the meantime, master
// regards it as a DEAD worker.
func (master *Master) CheckHeartbeatForEachWorker(workerID int64, heartbeat chan int) {
	for {
		timeout := time.After(HeartBeatTimeout)
		select {
		case <-timeout:
			// the worker fuck up, release the task
			log.Error("Worker: ", workerID, "fuck up")
			master.reportTaskFunc(master.workerTaskMap[workerID], ResultFailed)
			delete(master.workerTaskMap, workerID)
			return
		case heartbeatCode := <-heartbeat:
			// the worker is doing his job
			switch heartbeatCode {
			case HeartBeatCodeDead:
				log.Error("Worker: ", workerID, "fuck up")
				master.reportTaskFunc(master.workerTaskMap[workerID], ResultFailed)
				delete(master.workerTaskMap, workerID)
				return
			case HeartBeatCodeFinished:
				master.reportTaskFunc(master.workerTaskMap[workerID], ResultOK)
				return
			case HeartBeatCodePulse:
				continue
			}
		}
	}
}

type server struct {
	master *Master
}

// RequestTask is to deliver a task to worker.
func (s *server) RequestTask(ctx context.Context, in *kmrpb.RegisterParams) (*kmrpb.Task, error) {
	t, err := s.master.requestTaskFunc()
	s.master.heartbeat[in.WorkerID] = make(chan int, 10)
	s.master.workerTaskMap[in.WorkerID] = t
	go s.master.CheckHeartbeatForEachWorker(in.WorkerID, s.master.heartbeat[in.WorkerID])
	log.Infof("deliver a task Jobname: %v MapredNodeID: %v Phase: %v", t.JobNodeName, t.MapReduceNodeIndex, t.Phase)
	return &kmrpb.Task{
		Retcode: -1,
		Taskinfo: &kmrpb.TaskInfo{
			JobNodeName:     t.JobNodeName,
			MapredNodeIndex: t.MapReduceNodeIndex,
			Phase:           t.Phase,
			SubIndex:        int32(t.PhaseSubIndex),
		},
	}, err
}

// ReportTask is for executor to report its progress state to master.
func (s *server) ReportTask(ctx context.Context, in *kmrpb.ReportInfo) (*kmrpb.Response, error) {
	if _, ok := s.master.workerTaskMap[in.WorkerID]; !ok {
		log.Errorf("WorkerID %v is not working on anything", in.WorkerID)
		return &kmrpb.Response{Retcode: 0}, nil
	}

	var heartbeatCode int
	switch in.Retcode {
	case kmrpb.ReportInfo_FINISH:
		heartbeatCode = HeartBeatCodeFinished
	case kmrpb.ReportInfo_DOING:
		heartbeatCode = HeartBeatCodePulse
	case kmrpb.ReportInfo_ERROR:
		heartbeatCode = HeartBeatCodeDead
	default:
		panic("unknown ReportInfo")
	}
	go func(ch chan<- int) {
		ch <- heartbeatCode
	}(s.master.heartbeat[in.WorkerID])

	return &kmrpb.Response{Retcode: 0}, nil
}

// NewMaster Create a master, waiting for workers
func NewMaster(job *jobgraph.Job, port string) *Master {
	m := &Master{
		port:          port,
		workerTaskMap: make(map[int64]TaskDescription),
		heartbeat:     make(map[int64]chan int),
		job:           job,
	}

	m.scheduler = Scheduler{
		jobGraph: job,
	}

	m.requestTaskFunc, m.reportTaskFunc = m.scheduler.Schedule(m)

	go func() {
		lis, err := net.Listen("tcp", ":"+port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		log.Infof("listen localhost: %s", port)
		s := grpc.NewServer()
		kmrpb.RegisterMasterServer(s, &server{master: m})
		reflection.Register(s)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	m.heartbeat = make(map[int64]chan int)

	return m
}
