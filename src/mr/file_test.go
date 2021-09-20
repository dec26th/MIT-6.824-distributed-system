package mr

import (
	"fmt"
	"io/ioutil"
	"path"
	"testing"
)

func TestFile(t *testing.T) {
	files, _ := ioutil.ReadDir("../main/mr-tmp/")
	for _, file := range files {
		fmt.Println(file.Name())
		fmt.Println(path.Join("../main/mr-tmp/", file.Name()))
		fmt.Println(path.Base(path.Join("../main/mr-tmp/", file.Name())))
	}
}
