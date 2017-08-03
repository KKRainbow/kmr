package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/naturali/kmr/executor"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/pkg/aca"
	"github.com/naturali/kmr/pkg/path"
	"github.com/naturali/kmr/records"
)

var beginRune, endRune = rune('▶'), rune('◀')
var afterBeforeMap = make(map[rune][]rune)
var patternMap = make(map[string]uint64)

const (
	MAX_WORD_LENGTH = 20
)

type ExtendedSetMapper struct {
	mapred.MapperCommon
	inputCounter uint64
	counter      uint64
}

type ExtendedSetReduce struct {
	mapred.ReducerCommon
	counter uint64
}

func (m *ExtendedSetMapper) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	line := []rune(strings.Replace(string(key.(string)), " ", "", -1))
	line = append([]rune{beginRune}, line...)
	line = append(line, endRune)
	m.inputCounter++
	if m.counter > 8000000000 {
		return
	}

	handledMap := make(map[string]bool)

	ocurredChMap := make(map[rune]int)
	for idx, ch := range line {
		beforeArr, ok := afterBeforeMap[ch]
		if !ok {
			continue
		}
		for _, beforeCh := range beforeArr {
			if pos, ok := ocurredChMap[beforeCh]; !ok || idx-pos <= 1 {
				continue
			} else {
				if idx-pos > 5 {
					continue
				}
				str := string(line[pos+1 : idx])
				m.counter++

				strOccurs := 0
				if _, ok := handledMap[str]; !ok {
					handledMap[str] = true
					strOccurs = len(strings.Split(string(line), str)) - 1
				}
				output(str, strings.Join([]string{string(beforeCh), string(ch)}, " ")+","+strconv.Itoa(strOccurs))
			}
		}
		ocurredChMap[ch] = idx
	}
}

func Init() {
	matcher := aca.NewAhoCorasickMatcher()
	curPath, _ := path.Getcurdir()
	if name, err := os.Hostname(); err == nil && (name == "arch-sunsijie-linux") {
		curPath = "/home/sunsijie/Project/go/src/github.com/naturali/kmr/output"
	} else {
		curPath = "/output"
	}
	file, err := os.Open(filepath.Join(curPath, ".", "food.txt"))
	defer file.Close()
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)
	words := make([]string, 0)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		st := strings.Split(line, "\t")[0]
		if utf8.RuneCountInString(st) > 2 {
			words = append(words, st)
		}
	}
	matcher.Build(words)

	var rr records.RecordReader
	if name, err := os.Hostname(); err == nil && (name == "arch-sunsijie-linux") {
		rr = records.MakeRecordReader("file", map[string]interface{}{"filename": "/home/sunsijie/make_pair_output.t"})
	} else {
		rr = records.MakeRecordReader("file", map[string]interface{}{"filename": "/cephfs/kmr/make_pair_output/res-0.t"})
	}
	for rr.HasNext() {
		r := rr.Pop()
		if binary.LittleEndian.Uint64(r.Value) <= 100 {
			continue
		}
		pattern := string(r.Key)
		splited := strings.Split(pattern, " ")
		beforeCh, afterCh := []rune(splited[0]), []rune(splited[1])
		if splited[0] == "<s>" {
			beforeCh = []rune{beginRune}
		}
		if splited[1] == "</s>" {
			afterCh = []rune{endRune}
		}
		afterBeforeMap[afterCh[0]] = append(afterBeforeMap[afterCh[0]][:], beforeCh[0])
		patternMap[pattern] = binary.LittleEndian.Uint64(r.Value)
	}
}

func (r *ExtendedSetReduce) Reduce(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	var count uint32
	tmpMap := map[string]int{}
	wordTotalCount := 0
	mapred.ForEachValue(valuesNext, func(val interface{}) {
		p := val.(string)
		splited := strings.Split(p, ",")
		str := splited[0]
		num, err := strconv.Atoi(splited[1])
		if err != nil {
			panic(num)
		}
		wordTotalCount += num
		if _, exists := tmpMap[str]; !exists {
			tmpMap[str] = 1
			if _, patternExists := patternMap[str]; patternExists {
				count++
			}
		}
	})
	r.counter++
	if count != 0 {
		s := fmt.Sprintf("%v %d %d %f", key.(string), count, wordTotalCount, float64(count)/float64(wordTotalCount))
		output(s)
	}
}

func main() {
	wcmap := &ExtendedSetMapper{
		mapred.MapperCommon{
			mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.String{},
				InputValueTypeConverter:  mapred.Bytes{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.String{},
			},
		},
		0,
		0,
	}
	wcreduce := &ExtendedSetReduce{
		mapred.ReducerCommon{
			mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.String{},
				InputValueTypeConverter:  mapred.String{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.String{},
			},
		},
		0,
	}
	Init()
	cw := &executor.ComputeWrapClass{}
	cw.BindMapper(wcmap)
	cw.BindReducer(wcreduce)
	cw.Run()
}
