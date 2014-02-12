package configparser

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	Slaves         []string
	Master         string
	ReduceDataPort int
	Input          string
	InputDir       string
	Delimeter      string
	Username       string
	PEMFile        string
	MapDir         string
	ReduceDir      string
	NumMap         int
	NumReduce      int
	WebPort        int
	HTMLTemplate   string
}

func ParseFile(path string) Config {
	file, e := ioutil.ReadFile(path)
	if e != nil {
		panic(e)
	}
	var config Config
	json.Unmarshal(file, &config)
	return config
}
