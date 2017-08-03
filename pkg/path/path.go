package path

import (
	"fmt"
	"path/filepath"
	"runtime"
)

func Getcurdir() (string, error) {
	//get absolute path, which will be used to locate html and js files
	_, filename, _, _ := runtime.Caller(1)
	fmt.Println(filename)
	dir, err := filepath.Abs(filepath.Dir(filename))
	if err != nil {
		return "", err
	}
	return dir, nil
}
