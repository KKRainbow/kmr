package master

import (
	"errors"
	"fmt"
	"sync"

	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/util/log"
)

type TaskDescription struct {
	ID                 int
	JobNodeName        string
	MapReduceNodeIndex int32
	Phase              string
	PhaseSubIndex      int
}

type ReportFunction func(task TaskDescription, state int)
type RequestFunction func() (TaskDescription, error)
type PushJobFunction func(jobDesc jobgraph.JobDescription) (state <-chan int)

type EventHandler interface {
	TaskSucceeded(jobDesc *jobgraph.JobDescription) error
	TaskFailed(jobDesc *jobgraph.JobDescription) error
	MapReduceNodeSucceed(node *jobgraph.MapReduceNode) error
	MapReduceNodeFailed(node *jobgraph.MapReduceNode) error
	JobNodeSucceeded(node *jobgraph.JobNode) error
	JobNodeFailed(node *jobgraph.JobNode) error
}

const (
	StateIdle = iota
	StateInProgress
	StateCompleted

	ResultOK
	ResultFailed

	mapPhase    = "map"
	reducePhase = "reduce"
)

var NoAvailableJobError = errors.New("No available job")

// Scheduler Scheduler is responsible to schedule tasks.
// every MapReduce node can be divided into (nMappers*nReducer) tasks.
// executor request a task to execute.
type Scheduler struct {
	jobGraph *jobgraph.Job
}

type task struct {
	phase    string
	subIndex int

	job *mapReduceJob
}

type mapReduceJob struct {
	//Use this can get a unique mapredNode
	jobDesc               jobgraph.JobDescription
	mapTasks, reduceTasks []*task
}

func (s *Scheduler) CreateMapReduceTasks(desc jobgraph.JobDescription) (mrJob mapReduceJob, err error) {
	mrNode := s.jobGraph.GetMapReduceNode(desc.JobNodeName, int(desc.MapReduceNodeIndex))
	if mrNode == nil {
		err = errors.New(fmt.Sprint("Cannot get mapReduceNode from job graph, job description is", desc))
		return
	}
	mrJob.jobDesc = desc
	fillTask := func(job *mapReduceJob, phase string) {
		var nTasks int
		var batchSize int
		jobDesc := job.jobDesc
		switch phase {
		case mapPhase:
			nTasks = (jobDesc.MapperObjectSize + batchSize - 1) / batchSize
			batchSize = jobDesc.MapperBatchSize
		case reducePhase:
			nTasks = jobDesc.ReducerNumber
			batchSize = 1
		}

		tasks := make([]*task, nTasks)
		for i := 0; i < nTasks; i++ {
			tasks[i] = &task{
				phase:    phase,
				job:      job,
				subIndex: i,
			}
		}

		switch phase {
		case mapPhase:
			job.mapTasks = tasks
		case reducePhase:
			job.reduceTasks = tasks
		default:
			//XXX: should not be here
			log.Fatal("Unknown phase")
		}
	}
	fillTask(&mrJob, mapPhase)
	fillTask(&mrJob, reducePhase)
	return
}

func (s *Scheduler) Schedule(visitor EventHandler) (requestFunc RequestFunction, reportFunc ReportFunction) {
	jobResultChanMap := make(map[*mapReduceJob]<-chan int)
	availableJobs := make([]*mapReduceJob, 0)
	taskDescID := 0

	mapperFinishedMap := make(map[*mapReduceJob]int)
	reducerFinishedMap := make(map[*mapReduceJob]int)
	phaseMap := make(map[*mapReduceJob]string)
	taskStateMap := make(map[*task]int)
	taskIDMap := make(map[int]*task)

	type reportJobInput struct {
		desc  TaskDescription
		result int
	}
	type requestJobOutput struct {
		desc TaskDescription
		err  error
	}
	var (
		pushJobChan          = make(chan jobgraph.JobDescription, 1)
		requestJobChan       = make(chan int, 1)
		reportJobChan        = make(chan reportJobInput, 1)
		pushJobOutputChan    = make(chan (<-chan int), 1)
		requestJobOutputChan = make(chan requestJobOutput, 1)
	)
	select {
	case desc := <-pushJobChan:
		state := make(chan int, 1)
		j, err := s.CreateMapReduceTasks(desc)
		if err != nil {
			log.Fatal(err)
		}
		availableJobs = append(availableJobs, &j)
		jobResultChanMap[&j] = state
		pushJobOutputChan <- state
	case <-requestJobChan:
		for _, processingJob := range availableJobs {
			var tasks *[]*task
			if mapperFinishedMap[processingJob] == len(processingJob.mapTasks) {
				phaseMap[processingJob] = reducePhase
				tasks = &processingJob.reduceTasks
			} else if reducerFinishedMap[processingJob] < len(processingJob.reduceTasks) {
				phaseMap[processingJob] = mapPhase
				tasks = &processingJob.mapTasks
			} else {
				log.Fatal("After job node finished, this should not exist")
			}
			for _, task := range *tasks {
				if _, ok := taskStateMap[task]; !ok {
					taskStateMap[task] = StateIdle
				}
				if taskStateMap[task] == StateIdle {
					taskStateMap[task] = StateInProgress
					taskDescID++
					requestJobOutputChan <- requestJobOutput{
						TaskDescription{
							ID:                 taskDescID,
							JobNodeName:        processingJob.jobDesc.JobNodeName,
							MapReduceNodeIndex: processingJob.jobDesc.MapReduceNodeIndex,
							Phase:              phaseMap[processingJob],
							PhaseSubIndex:      task.subIndex,
						},
						nil,
					}
				}
			}
		}
		requestJobOutputChan <- requestJobOutput{TaskDescription{}, nil}
	case rep := <-reportJobChan:
		var t *task
		var ok bool
		if t, ok = taskIDMap[rep.desc.ID]; !ok || t == nil {
			log.Error("Report a task doesn't exists")
			return
		}
		if _, ok := taskStateMap[t]; !ok {
			log.Panic("Delivered task doesn't have a state")
			return
		}
		state := &taskStateMap[t]
		if rep.result == ResultOK {
			if *state != StateInProgress && *state != StateCompleted {
				log.Errorf("State of task reporting finished is not processing or completed")
			} else {
				if *state == StateInProgress {
					if phaseMap[t.job] == mapPhase {
						mapperFinishedMap[t.job]++
					} else {
						reducerFinishedMap[t.job]++
						if reducerFinishedMap[t.job] == len(t.job.reduceTasks) {
							curJobIdx := -1
							// remove this job
							for idx := range availableJobs {
								if availableJobs[idx] == t.job {
									curJobIdx = idx
								}
							}
							if curJobIdx >= 0 {
								availableJobs[curJobIdx] = availableJobs[len(availableJobs)-1]
								availableJobs = availableJobs[:len(availableJobs)-1]
								jobResultChanMap[t.job] <- ResultOK
							} else {
								log.Fatal("Cannot find job which is reporting to be done")
							}
						}
					}
				}
				*state = StateCompleted
			}
		} else {
			*state = StateIdle
		}
	}

	// PushJob Push a job to execute
	pushJobFunc := func(jobDesc jobgraph.JobDescription) <-chan int {
		pushJobChan <- jobDesc
		return <-pushJobOutputChan
	}

	requestFunc = func() (TaskDescription, error) {
		requestJobChan <- 1
		out := <-requestJobOutputChan
		return out.desc, out.err
	}

	reportFunc = func(desc TaskDescription, result int) {
		reportJobChan <- reportJobInput{desc, result}
	}

	go s.MapReduceNodeSchedule(pushJobFunc, visitor)
	return
}

func (s *Scheduler) MapReduceNodeSchedule(pushJobFunc PushJobFunction, eventHandler EventHandler) {

	waitForAll := &sync.WaitGroup{}
	jobStatusMap := make(map[*jobgraph.JobNode]int)
	mapsLock := sync.Mutex{}
	setJobNodeStatus := func(j *jobgraph.JobNode, status int) {
		mapsLock.Lock()
		defer mapsLock.Unlock()
		jobStatusMap[j] = status
	}
	statusEqualTo := func(j *jobgraph.JobNode, status int) bool {
		mapsLock.Lock()
		defer mapsLock.Unlock()
		if _, ok := jobStatusMap[j]; !ok {
			jobStatusMap[j] = StateIdle
		}
		return jobStatusMap[j] == status
	}
	// topo sort and push job to master
	var topo func(node *jobgraph.JobNode)
	topo = func(node *jobgraph.JobNode) {
		mrNodes := node.GetMapReduceNodes()
		for idx := 0; idx < len(mrNodes); {
			mrNode := mrNodes[idx]
			jobDesc := mrNode.ToJobDesc()
			if jobDesc == nil {
				log.Fatal("Failed to convert node ", jobDesc)
			}
			if result := <-pushJobFunc(*jobDesc); result == ResultOK {
				err := eventHandler.MapReduceNodeSucceed(mrNode)
				if err == nil {
					idx++
				} else {
					log.Error(err)
				}
			} else {
				err := eventHandler.MapReduceNodeFailed(mrNode)
				log.Error("Map reduce node failed", err, *mrNode)
			}
		}
		setJobNodeStatus(node, StateCompleted)
		for _, nextJobNode := range node.GetDependencyOf() {
			allDepCompleted := true
			for _, dep := range nextJobNode.GetDependencies() {
				if statusEqualTo(dep, StateCompleted) {
					allDepCompleted = false
				}
			}
			if !allDepCompleted {
				continue
			}
			if statusEqualTo(nextJobNode, StateIdle) {
				continue
			}
			setJobNodeStatus(nextJobNode, StateInProgress)
			waitForAll.Add(1)
			go topo(nextJobNode)
		}
		waitForAll.Done()
	}

	for _, n := range s.jobGraph.GetRootNodes() {
		if len(n.GetDependencies()) != 0 {
			continue
		}
		setJobNodeStatus(n, StateInProgress)
		waitForAll.Add(1)
		go topo(n)
	}
	waitForAll.Wait()
}
