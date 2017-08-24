package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"reflect"
	"strings"
	"github.com/naturali/kmr/util/log"
)

func overrideStructV(main reflect.Value, extra reflect.Value) {
	if main.Kind() == reflect.Ptr {
		if main.IsNil() && !extra.IsNil() {
			main.Set(extra)
			return
		}
		if !main.IsNil() && !extra.IsNil() && main.Elem().Kind() != reflect.Struct {
			main.Set(extra)
			return
		}
		main = main.Elem()
		extra = extra.Elem()
	}
	if extra.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < extra.NumField(); i++ {
		extraValueField := extra.Field(i)
		mainValueField := main.Field(i)

		if extraValueField.Kind() != reflect.Ptr {
			mainValueField.Set(extraValueField)
			continue
		}

		if mainValueField.IsNil() && !extraValueField.IsNil() {
			mainValueField.Set(extraValueField)
			continue
		}

		if !mainValueField.IsNil() && !extraValueField.IsNil() {
			overrideStructV(mainValueField, extraValueField)
		}
	}
}

func overrideStruct(main interface{}, extra interface{}) {
	if reflect.TypeOf(main).Name() != reflect.TypeOf(extra).Name() {
		panic("main and extra struct has different type")
	}
	overrideStructV(reflect.ValueOf(main).Elem(), reflect.ValueOf(extra).Elem())
}

// LoadConfigFromMultiFiles load config from files, right config will override left config
func LoadConfigFromMultiFiles(replacesMap map[string]string, configFiles ...string) *KMRConfig {
	config := &KMRConfig{}
	for _, file := range configFiles {
		if f, err := os.Open(file); err == nil {
			newconfig := &KMRConfig{}
			b, err := ioutil.ReadAll(f)
			if err != nil {
				continue
			}
			for k, v := range replacesMap {
				b = []byte(strings.Replace(string(b), k, v, -1))
			}
			err1 := json.Unmarshal(b, newconfig)
			if err1 != nil {
				log.Fatal(err1)
			}
			overrideStruct(config, newconfig)
		}
	}
	return config
}

func GetConfigLoadOrder() (configFiles []string) {
	configFiles = []string{
		"/etc/kmr/config.json",
	}

	// In cluster we don't have home dir
	if u, err := user.Current(); err == nil {
		configFiles = append(configFiles, path.Join(u.HomeDir, ".config/kmr/config.json"))
	}
	configFiles = append(configFiles, "./config.json")

	return
}
