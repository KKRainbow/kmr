package kmr

import "github.com/naturali/kmr/mapred"

const (
	mapperNode = iota
	reducerNode
)

type kmrDAGNode struct {
	nodeType     int
	mapper       mapred.Mapper
	reducer      mapred.Reducer
	dependencies []*kmrDAGNode
	dependencyOf []*kmrDAGNode
	constructor  *GraphConstructor
	visited      bool
}

type GraphConstructor struct {
	Root      []*kmrDAGNode
	LastAdded *kmrDAGNode
	allNodes  []*kmrDAGNode
}

type MapJobDescription struct {
}

func Run() {

}

var constructor GraphConstructor

func (node *kmrDAGNode) AddMapper(mapper mapred.Mapper) *kmrDAGNode {
	if node.nodeType == mapperNode {
		node.mapper = mapred.CombineMappers(node.mapper, mapper)
		return node
	}
	newNode := &kmrDAGNode{
		nodeType:     mapperNode,
		mapper:       mapper,
		dependencies: []*kmrDAGNode{node},
		constructor:  node.constructor,
	}
	if node.constructor == nil {
		panic("node constructor is nil")
	}
	node.constructor.allNodes = append(node.constructor.allNodes, newNode)
	node.dependencyOf = append(node.dependencyOf, newNode)
	return newNode
}

func (node *kmrDAGNode) AddReducer(reducer mapred.Reducer) interface{} {
	newNode := &kmrDAGNode{
		nodeType:     reducerNode,
		reducer:      reducer,
		dependencies: []*kmrDAGNode{node},
		constructor:  node.constructor,
	}
	if node.constructor == nil {
		panic("node constructor is nil")
	}
	node.constructor.allNodes = append(node.constructor.allNodes, newNode)
	node.dependencyOf = append(node.dependencyOf, newNode)
	return newNode
}

func AddMapper(mapper mapred.Mapper) *kmrDAGNode {
	c := &kmrDAGNode{
		nodeType:    mapperNode,
		mapper:      mapper,
		constructor: &constructor,
	}
	constructor.Root = append(constructor.Root, c)
	constructor.allNodes = append(constructor.allNodes, c)
	return c
}

func Schedule() <-chan interface{} {
	output := make(chan interface{}, 1)
	for _, c := range constructor.allNodes {
		c.visited = false
	}
	var dfs func(*kmrDAGNode)
	dfs = func(node *kmrDAGNode) {
		node.visited = true
		for _, n := range node.dependencies {
			dfs(n)
		}
		if node.nodeType == mapperNode {
			output <- node.mapper
		} else if node.nodeType == reducerNode {
			output <- node.reducer
		}
	}
	for _, n := range constructor.allNodes {
		dfs(n)
	}
	close(output)
	return output
}
