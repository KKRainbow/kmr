package kmr

import "github.com/naturali/kmr/mapred"

const (
	mapperNode = iota
	reducerNode
)

type kmrDAGNode struct {
	nodeType      int
	nodeName      string
	mapper        mapred.Mapper
	reducer       mapred.Reducer
	dependencies  []*kmrDAGNode
	dependencyOf  []*kmrDAGNode
	visited       bool
	graph         *JobGraph
	subjobCount   int
	processingMap []byte
	finishdMap    []byte
	finishedCount int
}

type JobGraph struct {
	Root          []*kmrDAGNode
	allNodes      []*kmrDAGNode
	topoSortedArr []*kmrDAGNode
}

type JobDescriptor struct {
	Empty     bool
	nodeName  string
	subJobNum int
}

type MapJobDescription struct {
}

func Run() {

}

func (jg *JobGraph) getTopoSortedNodeArray() []*kmrDAGNode {
	topoSortedArr := make([]*kmrDAGNode, len(jg.allNodes))
	for _, c := range jg.allNodes {
		c.visited = false
	}
	idx := 0
	var dfs func(*kmrDAGNode)
	dfs = func(node *kmrDAGNode) {
		if node.visited {
			return
		}
		node.visited = true
		for _, n := range node.dependencies {
			dfs(n)
		}
		topoSortedArr[idx] = node
		idx++
	}
	for _, n := range jg.allNodes {
		dfs(n)
	}
	return topoSortedArr
}

func (jg *JobGraph) kmrGetIncompletedNode() *kmrDAGNode {
	if len(jg.topoSortedArr) != len(jg.allNodes) {
		jg.topoSortedArr = jg.getTopoSortedNodeArray()
	}
	for idx, node := range jg.topoSortedArr {
		if node.finishedCount == node.subjobCount {
			continue
		}
		allDepFinishd := true
		for _, dep := range node.dependencies {
			if dep.finishedCount != dep.subjobCount {
				allDepFinishd = false
				break
			}
		}
		if allDepFinishd {
			return node
		}
	}
	return nil
}

func (jg *JobGraph) GetNextJob() JobDescriptor {
	node := jg.kmrGetIncompletedNode()
	if node == nil {
		return JobDescriptor{
			true,
		}
	}
}

func (node *kmrDAGNode) AddMapper(mapper mapred.Mapper) *kmrDAGNode {
	if node.nodeType == mapperNode {
		node.mapper = mapred.CombineMappers(node.mapper, mapper)
		return node
	}
	newNode := &kmrDAGNode{
		nodeType:     mapperNode,
		mapper:       mapper,
		dependencies: []*kmrDAGNode{node},
		graph:        node.graph,
	}
	if node.constructor == nil {
		panic("node constructor is nil")
	}
	node.constructor.allNodes = append(node.constructor.allNodes, newNode)
	node.dependencyOf = append(node.dependencyOf, newNode)
	return newNode
}

func (node *kmrDAGNode) AddReducer(reducer mapred.Reducer) interface{} {
	if node.nodeType == reducerNode {
		panic("reducer should follow a reducer")
	}
	newNode := &kmrDAGNode{
		nodeType:     reducerNode,
		reducer:      reducer,
		dependencies: []*kmrDAGNode{node},
		graph:        node.graph,
	}
	if node.constructor == nil {
		panic("node constructor is nil")
	}
	node.constructor.allNodes = append(node.constructor.allNodes, newNode)
	node.dependencyOf = append(node.dependencyOf, newNode)
	return newNode
}

func (j *JobGraph) AddMapper(mapper mapred.Mapper) *kmrDAGNode {
	c := &kmrDAGNode{
		nodeType: mapperNode,
		mapper:   mapper,
		graph:    j,
	}
	j.Root = append(j.Root, c)
	j.allNodes = append(j.allNodes, c)
	return c
}

type FileDepGraph struct {
}

// ComputeGraphDetail generate a job description DAG graph which is depended on file dependencies
// Generated FileDepGraph can be used to deliver job to worker
func convertToFileDependencyGraph(GraphConstructor graph, inputs ...string) *FileDepGraph {
	return nil
}

type JobScheduler struct {
	fileDAG *FileDepGraph
}

func GetJobScheduler() *JobScheduler {
	return nil
}

func (*JobScheduler) GetJob(int id) interface{} {
	return nil
}

// ReportFileGenerated After a worker is completed and file is generated
func (*JobScheduler) ReportFileGenerated() (interface{}, error) {
	return nil, nil
}
