package main

import (
	"unicode"

	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/cli"
)

func isAlphaOrNumber(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || unicode.IsDigit(r)
}

func isChinese(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fa5'
}

func ProcessSingleSentence(line string) []string {
	outputs := make([]string, 0)
	e_word := ""
	for _, r := range line {
		if isChinese(r) {
			if len(e_word) > 0 {
				outputs = append(outputs, e_word)
				e_word = ""
			}
			outputs = append(outputs, string(r))
		} else if isAlphaOrNumber(r) {
			e_word += string(r)
		} else {
			if len(e_word) > 0 {
				outputs = append(outputs, e_word)
				e_word = ""
			}
		}
	}
	if len(e_word) > 0 {
		outputs = append(outputs, e_word)
	}
	return outputs
}

type WordCountMapper struct {
	mapred.MapperCommon
	judgeFunc func(ch rune) bool
}

// Map Value is lines from file. Map function split lines into words and emit (word, 1) pairs
func (mapper *WordCountMapper) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	v, _ := value.(string)
	words := ProcessSingleSentence(v)
	for _, word := range words {
		output(word, uint32(1))
	}
}

type WordCountReducer struct {
	mapred.ReducerCommon
}

// Reduce key is word and valueNext is an iterator function. Add all values of one key togather to count the word occurs
func (*WordCountReducer) Reduce(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	var count uint32
	mapred.ForEachValue(valuesNext, func(value interface{}) {
		val, _ := value.(uint32)
		count += val
	})
	output(count)
}

func main() {
	wcmap := &WordCountMapper{
		MapperCommon: mapred.MapperCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
				InputValueTypeConverter:  mapred.String{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint32{},
			},
		},
	}
	wcreduce := &WordCountReducer{
		ReducerCommon: mapred.ReducerCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
				InputValueTypeConverter:  mapred.Uint32{},
				OutputKeyTypeConverter:   mapred.Bytes{},
				OutputValueTypeConverter: mapred.Uint32{},
			},
		},
	}
	var job jobgraph.Job
	job.SetName("word-count")

	inputs := &jobgraph.InputFiles{
		// put a.t in the map bucket directory
		Files: []string{"a.t"},
		Type:  "textstream",
	}

	job.AddJobNode(inputs, "WordCount").
		AddMapper(wcmap, 1).
		AddReducer(wcreduce, 10)

	cli.Run(&job)
}
