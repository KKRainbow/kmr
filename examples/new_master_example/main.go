package main

import (
	"github.com/naturali/kmr/kmr"
	"github.com/naturali/kmr/mapred"
)

type MyMap struct {
	mapred.MapReduceBase
}

type MyReduce struct {
	mapred.MapReduceBase
}

func (*MyMap) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
}

func (*MyReduce) Reduce(key interface{}, valuesNext func() (interface{}, error), output func(k interface{}, v interface{}), reporter interface{}) {
}

func main() {
	//Here I show the way I would like to write a mapreduce program

	//Following describe our job, a DAG
	firstMapper := kmr.Mapper(&MyMap{})
	//do something with firstMapperName

	secondMapper := firstMapper.Mapper(&MyMap{})

	//Run will set the map object from json file or console argument
	kmr.Run()
}
