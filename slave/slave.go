package slave

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/spicavigo/gomr/configparser"
	"github.com/spicavigo/gomr/task"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"reflect"
)

type Args struct{}
type ReduceArgs struct{}
type PathArgs struct {
	Path string
}
type StatusArgs struct{}
type StatusResult struct {
	NumMap        int
	NumReduce     int
	MapRunning    int
	MapDone       int
	ReduceRunning int
	ReduceDone    int
}
type Job struct {
	path           string
	NumMap         int
	NumReduce      int
	MapRunning     int
	MapDone        int
	ReduceRunning  int
	ReduceDone     int
	ReduceDataPort int
	Master         string
}

type CombineCollector struct {
	Key   interface{}
	Value interface{}
}

var Port string

func combine(mapData interface{}, combined interface{}) {
	vxs := reflect.ValueOf(mapData)
	combinedTemp := reflect.ValueOf(combined)

	for i := 0; i < vxs.Len(); i++ {
		key := vxs.Index(i).Field(0)
		value := vxs.Index(i).Field(1)

		currval := combinedTemp.MapIndex(key)

		if currval.IsValid() {
			combinedTemp.SetMapIndex(key, reflect.Append(currval, value))
		} else {
			n := reflect.SliceOf(value.Type())
			n2 := reflect.MakeSlice(n, 0, 100)
			combinedTemp.SetMapIndex(key, reflect.Append(n2, value))
		}
	}
}

func (t *Job) sendReduceData(data []task.ReduceCollector) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", t.Master, t.ReduceDataPort))
	if err != nil {
		log.Fatal("Connection error ", err)
	}
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(&Port)
	err = encoder.Encode(&data)
	if err != nil {
		panic(err)
	}
	conn.Close()
}

func mapRoutine(chin chan string, chout chan []task.MapCollector, chfin chan bool) {
	for {
		select {
		case data := <-chin:
			chout <- task.Map(data)
		case <-chfin:
			return
		}
	}
}

func reduceRoutine(chin chan CombineCollector, chout chan task.ReduceCollector, chfin chan bool) {
	r := reflect.ValueOf(task.Reduce)
	for {
		select {
		case data := <-chin:
			key := reflect.ValueOf(data.Key)
			val := reflect.ValueOf(data.Value)
			chout <- r.Call([]reflect.Value{key, val})[0].Interface().(task.ReduceCollector)
		case <-chfin:
			return
		}
	}

}

func copystr(a string) string {
	if len(a) == 0 {
		return ""
	}
	return a[0:1] + a[1:]
}

func (t *Job) Map(args Args, reply *int) error {
	resetJob(t)
	go func() { //Main Anon func
		data := splitData(t)
		length := len(data)
		if len(data) == 0 {
			return
		}
		chin := make(chan string, len(data))
		chout := make(chan []task.MapCollector, len(data))
		chfin := make(chan bool)

		for i := 0; i < t.NumMap; i++ {
			go mapRoutine(chin, chout, chfin)
		}

		for i := range data {
			chin <- copystr(data[i])
		}
		data = nil

		// Get a map
		var mapColl task.MapCollector
		elem := reflect.TypeOf(mapColl)
		key := elem.Field(0).Type
		value := reflect.SliceOf(elem.Field(1).Type)
		mtype := reflect.MapOf(key, value)
		minst := reflect.MakeMap(mtype).Interface()
		mpv := reflect.ValueOf(minst)

		for i := 0; i < length; i++ {
			mapColl := <-chout
			for j := 0; j < len(mapColl); j++ {
				key := reflect.ValueOf(mapColl[j].Key)
				value := reflect.ValueOf(mapColl[j].Value)

				currval := mpv.MapIndex(key)

				if currval.IsValid() {
					mpv.SetMapIndex(key, reflect.Append(currval, value))
				} else {
					n := reflect.SliceOf(value.Type())
					n2 := reflect.MakeSlice(n, 1, 1)
					mpv.SetMapIndex(key, reflect.Append(n2, value))
				}

			}
		}

		for i := 0; i < t.NumMap; i++ {
			chfin <- true
		}
		close(chfin)
		close(chout)
		close(chin)
		keys := mpv.MapKeys()
		length = len(keys)
		chinr := make(chan CombineCollector, length)
		choutr := make(chan task.ReduceCollector, length)
		chfinr := make(chan bool)

		for i := 0; i < t.NumReduce; i++ {
			go reduceRoutine(chinr, choutr, chfinr)
		}

		for i := range keys {
			key := keys[i]
			val := mpv.MapIndex(key)
			chinr <- CombineCollector{key.Interface(), val.Interface()}
		}

		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", t.Master, t.ReduceDataPort))
		if err != nil {
			log.Fatal("Connection error ", err)
		}
		encoder := gob.NewEncoder(conn)
		err = encoder.Encode(&Port)
		err = encoder.Encode(&length)

		for _ = range keys {
			redData := <-choutr
			err = encoder.Encode(&redData)

		}

		if err != nil {
			panic(err)
		}
		conn.Close()
		for i := 0; i < t.NumReduce; i++ {
			chfinr <- true
		}
	}()
	*reply = 0
	return nil
}

func (t *Job) decodeReduce(filename string) []task.ReduceCollector {
	n, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	p := bytes.NewBuffer(n)
	dec := gob.NewDecoder(p)
	var e []task.ReduceCollector
	err = dec.Decode(&e)
	if err != nil {
		panic(err)
	}
	os.Remove(filename)
	return e
}

func (t *Job) Reduce(args Args, reply *int) error {
	go func() {
		r := new(task.ReduceCollector)
		elem := reflect.TypeOf(r).Elem()
		key := elem.Field(0).Type
		value := reflect.SliceOf(elem.Field(1).Type)
		m := reflect.MapOf(key, value)
		mp := reflect.MakeMap(m).Interface()

		combine(t.decodeReduce(t.path), mp)

		mpv := reflect.ValueOf(mp)
		keys := mpv.MapKeys()
		length := len(keys)
		chinr := make(chan CombineCollector, length)
		choutr := make(chan task.ReduceCollector, length)
		chfinr := make(chan bool)

		for i := 0; i < t.NumReduce; i++ {
			go reduceRoutine(chinr, choutr, chfinr)
		}

		for i := range keys {
			key := keys[i]
			val := mpv.MapIndex(key)
			chinr <- CombineCollector{key.Interface(), val.Interface()}
		}

		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", t.Master, t.ReduceDataPort))
		if err != nil {
			log.Fatal("Connection error ", err)
		}
		encoder := gob.NewEncoder(conn)
		err = encoder.Encode(&Port)
		err = encoder.Encode(&length)

		for _ = range keys {
			redData := <-choutr
			err = encoder.Encode(&redData)

		}
		if err != nil {
			panic(err)
		}
		conn.Close()
		for i := 0; i < t.NumReduce; i++ {
			chfinr <- true
		}
	}()

	*reply = 0
	return nil
}

func (t *Job) Status(args StatusArgs, reply *StatusResult) error {
	reply.NumMap = t.NumMap
	reply.NumReduce = t.NumReduce
	reply.MapRunning = t.MapRunning
	reply.MapDone = t.MapDone
	reply.ReduceRunning = t.ReduceRunning
	reply.ReduceDone = t.ReduceDone
	return nil
}

func (t *Job) SetPath(args PathArgs, reply *int) error {
	t.path = args.Path
	*reply = 0
	return nil
}

func splitData(t *Job) []string {
	data := task.InputParser(t.path)
	os.Remove(t.path)
	return data
}

func resetJob(t *Job) {
	t.MapRunning = 0
	t.MapDone = 0
	t.ReduceRunning = 0
	t.ReduceDone = 0
}

func Run(configFile string, port string) {

	t := new(Job)
	resetJob(t)
	config := configparser.ParseFile(configFile)
	t.NumMap = config.NumMap
	t.NumReduce = config.NumReduce
	t.Master = config.Master
	t.ReduceDataPort = config.ReduceDataPort

	os.RemoveAll(config.MapDir)
	os.RemoveAll(config.ReduceDir)
	os.Mkdir(config.MapDir, 0755)
	os.Mkdir(config.ReduceDir, 0755)

	rpc.Register(t)

	listener, e := net.Listen("tcp", fmt.Sprintf(":%s", port))
	Port = port

	if e != nil {
		log.Fatal("listen error:", e)
	}
	for {
		if conn, err := listener.Accept(); err != nil {
			log.Fatal("accept error: " + err.Error())
		} else {
			go rpc.ServeConn(conn)
		}
	}
}
