package master

import (
	"errors"
	"fmt"
	"sync"

	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/util/log"
	"github.com/naturali/kmr-new/master"
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
type MapReduceJobVisitFunction func(jobDesc jobgraph.JobDescription) (state <-chan int)

type TaskVisitor interface {
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
)

var NoAvailableJobError = errors.New("No available job")

// Scheduler Scheduler is responsible to schedule tasks.
// every MapReduce node can be divided into (nMappers*nReducer) tasks.
// executor request a task to execute.
type Scheduler struct {
	taskDescID            int
	jobGraph              *jobgraph.Job
	jobStatus             map[int]int
	availableMapReduceJob []*mapReduceJob
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

func (s *Scheduler) ConvertToMapReduceJob(desc jobgraph.JobDescription) (mrJob mapReduceJob, err error) {
	mrNode := s.jobGraph.GetMapReduceNode(desc.JobNodeName, int(desc.MapReduceNodeIndex))
	if mrNode == nil {
		err = errors.New(fmt.Sprint("Cannot get mapReduceNode from job graph, job description is", desc))
		return
	}
	mrJob.jobDesc = desc
	fillTask := func(j *mapReduceJob, phase string) {
		var nTasks int
		var batchSize int
		jobDesc := j.jobDesc
		switch phase {
		case mapPhase:
			batchSize = jobDesc.MapperBatchSize
			nTasks = (jobDesc.MapperObjectSize + batchSize - 1) / batchSize
		case reducePhase:
			nTasks = jobDesc.ReducerNumber
			batchSize = 1
		}

		tasks := make([]*task, nTasks)
		for i := 0; i < nTasks; i++ {
			tasks[i] = &task{
				phase:    phase,
				job:      j,
				subIndex: i,
			}
		}

		if phase == mapPhase {
			j.mapTasks = tasks
		} else if phase == reducePhase {
			j.reduceTasks = tasks
		} else {
			//XXX: should not be here
			log.Fatal("Unknown phase")
		}
	}
	fillTask(&mrJob, mapPhase)
	fillTask(&mrJob, reducePhase)
	return
}

func (s *Scheduler) Schedule(visitor TaskVisitor) (requestFunc RequestFunction, reportFunc ReportFunction) {
	jobResultChanMap := make(map[*mapReduceJob]<-chan int)
	availableJobs := make([]*mapReduceJob, 0)
	availableJobsLock := sync.Mutex{}
	/*
		type ReportFunction func(task TaskDescription, state int)
		type MapReduceJobVisitFunction func(jobDesc jobgraph.JobDescription) (state <-chan int)
	*/
	// PushJob Push a job to execute
	pushJobFunc := func(jobDesc jobgraph.JobDescription) (state <-chan int) {
		state = make(chan int, 1)

		j, err := s.ConvertToMapReduceJob(jobDesc)
		if err != nil {
			log.Fatal(err)
		} //Which phase has been executing

		availableJobsLock.Lock()
		defer availableJobsLock.Unlock()
		availableJobs = append(availableJobs, &j)

		jobResultChanMap[&j] = state

		return
	}
	go s.MapReduceNodeSchedule(pushJobFunc, visitor)

	mapperFinishedMap := make(map[*mapReduceJob]int)
	reducerFinishedMap := make(map[*mapReduceJob]int)
	phaseMap := make(map[*mapReduceJob]string)
	taskStateMap := make(map[*task]int)
	taskIDMap := make(map[int]*task)

	requestFunc = func() (TaskDescription, error) {
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
					s.taskDescID++
					return TaskDescription{
						ID:                 s.taskDescID,
						JobNodeName:        processingJob.jobDesc.JobNodeName,
						MapReduceNodeIndex: processingJob.jobDesc.MapReduceNodeIndex,
						Phase:              phaseMap[processingJob],
						PhaseSubIndex:      task.subIndex,
					}, nil
				}
			}
		}
		return TaskDescription{}, NoAvailableJobError
	}

	reportFunc = func(desc TaskDescription, result int) {
		var t *task
		if t, ok := taskIDMap[desc.ID]; !ok || t == nil {
			log.Error("Report a task doesn't exists")
			return
		}
		if _, ok := taskStateMap[t]; !ok {
			log.Panic("Delivered task doesn't have a state")
			return
		}
		state := &taskStateMap[t]
		if result == ResultOK {
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
	return
}

func (s *Scheduler) MapReduceNodeSchedule(visitorFunc MapReduceJobVisitFunction, visitor TaskVisitor) {

	waitForAll := &sync.WaitGroup{}
	jobNodeLockMap := make(map[*jobgraph.JobNode]sync.Mutex)
	jobStatusMap := make(map[*jobgraph.JobNode]int)
	changeJobNodeStatus := func(j *jobgraph.JobNode, status int) {
		if _, ok := jobNodeLockMap[j]; !ok {
			jobNodeLockMap[j] = sync.Mutex{}
		}
		jobNodeLockMap[j].Lock()
		defer jobNodeLockMap[j].Unlock()
		jobStatusMap[j] = status
	}
	statusEqualTo := func(j *jobgraph.JobNode, status int) bool {
		if _, ok := jobNodeLockMap[j]; !ok {
			jobNodeLockMap[j] = sync.Mutex{}
		}
		if _, ok := jobStatusMap[j]; !ok {
			jobStatusMap[j] = StateIdle
		}
		jobNodeLockMap[j].Lock()
		defer jobNodeLockMap[j].Unlock()
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
			wait := visitorFunc(*jobDesc)
			result := <-wait
			if result == ResultOK {
				/*
					for x := 0; x < startNode.reducerCount; x++ {
						for _, file := range startNode.interFiles.getReducerInputFiles(x) {
							j.reduceBucket.Delete(file)
						}
					}
					if startNode.chainPrev != nil {
						for _, file := range startNode.chainPrev.outputFiles.GetFiles() {
							j.reduceBucket.Delete(file)
						}
					}
				*/
				err := visitor.MapReduceNodeSucceed(mrNode)
				if err == nil {
					idx++
				} else {
					log.Error(err)
				}
			} else {
				err := visitor.MapReduceNodeFailed(mrNode)
				log.Error("Map reduce node failed", err, *mrNode)
			}
		}
		changeJobNodeStatus(node, StateCompleted)
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
			changeJobNodeStatus(nextJobNode, StateInProgress)
			waitForAll.Add(1)
			go topo(nextJobNode)
		}
		waitForAll.Done()
	}

	for _, n := range s.jobGraph.GetRootNodes() {
		if len(n.GetDependencies()) != 0 {
			continue
		}
		changeJobNodeStatus(n, StateInProgress)
		waitForAll.Add(1)
		go topo(n)
	}
	waitForAll.Wait()
}
