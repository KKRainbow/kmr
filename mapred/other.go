package mapred
import (
	"io"
)

type Filter interface {
	InputTypeConverter
	Filter(k, v interface{}, writer io.Writer)
	Init()
	After()
}

type FilterCommon struct {
	InputKeyTypeConverter    TypeConverter
	InputValueTypeConverter  TypeConverter
}

func (*FilterCommon) Init() {}
func (*FilterCommon) After() {}

type BashCommand interface {
	InputTypeConverter
	RunCommand()
	Init()
	After()
}

type BashCommandBase struct {
	InputKeyTypeConverter    TypeConverter
	InputValueTypeConverter  TypeConverter
}

func (*BashCommandBase) Init() {}
func (*BashCommandBase) After() {}
