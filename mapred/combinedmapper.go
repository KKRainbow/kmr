package mapred

type combinedMapper struct {
	MapperCommon
	FirstMapper  Mapper
	SecondMapper Mapper
}

func (cm *combinedMapper) Map(key interface{}, value interface{}, output func(k interface{}, v interface{}), reporter interface{}) {
	cm.FirstMapper.Map(key, value, func(k, v interface{}) {
		if cm.SecondMapper != nil {
			cm.SecondMapper.Map(k, v, output, reporter)
		} else {
			output(k, v)
		}
	}, reporter)
}

//CombineMappers combine multiple mappers into one
func CombineMappers(mappers ...Mapper) Mapper {
	i := len(mappers)
	if i == 0 {
		panic("Number of mappers being combind should not be zero")
	} else if i == 1 {
		return mappers[0]
	} else {
		return &combinedMapper{
			FirstMapper:  mappers[0],
			SecondMapper: CombineMappers(mappers[1:]...),
		}
	}
}
