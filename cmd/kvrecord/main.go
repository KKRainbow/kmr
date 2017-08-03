package main

import (
	"fmt"
	"os"
	"encoding/binary"
	"bytes"

	"github.com/naturali/kmr/records"
)

func main() {
	if len(os.Args) == 2 {
		rr := records.MakeRecordReader("file", map[string]interface{}{"filename": os.Args[1]})
		for rr.HasNext() {
			r := rr.Pop()
			fmt.Println(string(r.Key), string(r.Value))
			//fmt.Println(string(r.Key), FromBytes(r.Value).(uint32))
		}
	} else {
		fmt.Println("Usage: kvrecord <filename>")
	}
}

// FromBytes int32 to bytes
func FromBytes(b []byte) interface{} {
	if len(b) != 4 {
		fmt.Println("not 4 byte but", len(b))
		panic(b)
	} else {
		var val uint32
		err := binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &val);
		if err == nil {
			return val
		}
		panic(err)
	}
	return nil
}
