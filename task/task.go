package task

import "fmt"
import "strings"
import "io/ioutil"

type MapCollector struct {
	Key   string
	Value int
}

type ReduceCollector struct {
	Key   string
	Value int
}

func Map(value string) []MapCollector {
	//fmt.Println("Map Called")
	//fmt.Println(value)
	tokens := strings.Split(value, " ")
	ret := make([]MapCollector, len(tokens))
	for i := range tokens {
		ret[i] = MapCollector{tokens[i], 1}
	}
	return ret
}

func Reduce(key string, value []int) ReduceCollector {
	//fmt.Println("reduce: ", key, value)
	return ReduceCollector{key, sum(value)}
}

func InputParser(path string) []string {
	content, _ := ioutil.ReadFile(path)
	lines := strings.Split(string(content), "\n")
	return lines
}

func OutputWriter(data []ReduceCollector) {
	fmt.Println(data)
}

func sum(a []int) (s int) {
	for _, v := range a {
		s += v
	}
	return
}
