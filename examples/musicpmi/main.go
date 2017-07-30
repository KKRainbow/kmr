package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/naturali/kmr/executor"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/pkg/aca"
)

var musicMatcher, artistMatcher *aca.AhoCorasickMatcher
var musicArtistMap = make(map[string][]string)

func Init() {
	musicMatcher = aca.NewAhoCorasickMatcher()
	artistMatcher = aca.NewAhoCorasickMatcher()
	var musicListFileDir string
	if name, err := os.Hostname(); err == nil && (name == "arch-sunsijie-linux") {
		musicListFileDir = "/home/sunsijie/Project/go/src/github.com/naturali/kmr/output2/"
	} else {
		musicListFileDir = "/mnt/cephfs"
	}
	file, err := os.Open(filepath.Join(musicListFileDir, ".", "filtered_music"))
	defer file.Close()
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)
	musics := make([]string, 0)
	artists := make([]string, 0)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		splited := strings.Split(line, ",")
		if len(splited) != 2 {
			panic(fmt.Sprintln("Music csv malformated with line", line))
		}
		music := splited[0]
		artist := splited[1]
		if utf8.RuneCountInString(music) > 0 {
			musics = append(musics, music)
			musicArtistMap[music] = append(musicArtistMap[music], artist)
		}
		if utf8.RuneCountInString(artist) > 0 {
			artists = append(artists, artist)
		} else {
			artist = "Unknown"
		}
	}
	musicMatcher.Build(musics)
	artistMatcher.Build(artists)
}
func main() {
	Init()
	cw := &executor.ComputeWrapClass{}
	cw.BindMapper(wcmap)
	cw.BindReducer(wcreduce)
	cw.Run()
}

func Init() {
	musicMatcher = aca.NewAhoCorasickMatcher()
	artistMatcher = aca.NewAhoCorasickMatcher()
	var musicListFileDir string
	if name, err := os.Hostname(); err == nil && (name == "arch-sunsijie-linux") {
		musicListFileDir = "/home/sunsijie/Project/go/src/github.com/naturali/kmr/output2/"
	} else {
		musicListFileDir = "/mnt/cephfs"
	}
	file, err := os.Open(filepath.Join(musicListFileDir, ".", "filtered_music"))
	defer file.Close()
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)
	musics := make([]string, 0)
	artists := make([]string, 0)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		splited := strings.Split(line, ",")
		if len(splited) != 2 {
			panic(fmt.Sprintln("Music csv malformated with line", line))
		}
		music := splited[0]
		artist := splited[1]
		if utf8.RuneCountInString(music) > 0 {
			musics = append(musics, music)
			musicArtistMap[music] = append(musicArtistMap[music], artist)
		}
		if utf8.RuneCountInString(artist) > 0 {
			artists = append(artists, artist)
		} else {
			artist = "Unknow"
		}
	}
	musicMatcher.Build(musics)
	artistMatcher.Build(artists)
}

func CountMusicMap(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	line := strings.Replace(string(key.(string)), " ", "", -1)
	strs, idxs := musicMatcher.Match(line)
	for _, m := range strs {
		output(m, 1)
	}
}

func CountArtistMap(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	line := strings.Replace(string(key.(string)), " ", "", -1)
	strs, idxs := artistMatcher.Match(line)
	for _, m := range strs {
		output(m, 1)
	}
}

func CountPairMap(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	line := strings.Replace(string(key.(string)), " ", "", -1)
	musics, _ := artistMatcher.Match(line)
	artists, _ := musicMatcher.Match(line)
	for _, m := range musics {
		for _, artist := range artists {
			for _, artistOfMusic := range musicArtistMap[m] {
				if artist == artistOfMusic {
					output(strings.Join([]string{m, artist}, ","), 1)
				}
			}
		}
	}
}

func AggregateReducer(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	var counter uint32
	mapred.ForEachValue(valuesNext, func(v interface{}) {
		counter += v.(uint32)
	})
	output(counter)
}

func PMIReducer(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	countMusicOutputs := 
}
