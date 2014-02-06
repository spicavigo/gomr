package master

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/gnicod/goscplib"
	"github.com/spicavigo/gomr/configparser"
	"github.com/spicavigo/gomr/task"
	"github.com/spicavigo/gomr/util"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type password string

func (p password) Password(_ string) (string, error) {
	return string(p), nil
}

type Master struct {
	Slaves         []string
	IdleSlaves     []string
	BusySlaves     []string
	ReduceDataPort int
	InputF         string
	InputDelim     byte
	MapDir         string
	ReduceDir      string
	Username       string
	PEMFile        string
}

type Args struct{}
type PathArgs struct {
	Path string
}

func getPath(base string) string {
	return filepath.Join(base, fmt.Sprintf("%d", rand.Int()))
}

func NewMaster(config configparser.Config) *Master {
	master := Master{
		Slaves:         config.Slaves,
		IdleSlaves:     config.Slaves,
		BusySlaves:     make([]string, len(config.Slaves)),
		ReduceDataPort: config.ReduceDataPort,
		InputF:         config.Input,
		InputDelim:     byte(config.Delimeter[0]),
		MapDir:         config.MapDir,
		ReduceDir:      config.ReduceDir,
		Username:       config.Username,
		PEMFile:        config.PEMFile,
	}
	return &master
}

func (m *Master) setPath(slave string, path string) {
	p := &PathArgs{path}
	m.slaveMRCall(slave, "Job.SetPath", p)
}

func (m *Master) getAddr(ip string) string {
	for _, a := range m.Slaves {
		if strings.HasPrefix(a, ip) {
			return a
		}
	}
	return ip
}

func (m *Master) slaveMRCall(slave string, method string, args interface{}) {
	client, err := rpc.Dial("tcp", slave)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply int
	err = client.Call(method, args, &reply)
	if err != nil {
		log.Fatal("RPC MR Slave error:", err)
	}
}

func (m *Master) scpFile(path string, dest string, destPath string) {

	sshConn := util.GetSSHConn(strings.Split(dest, ":")[0], m.Username, m.PEMFile)
	scpConn := goscplib.NewScp(sshConn)
	scpConn.PushFile(path, destPath)
	sshConn.Close()
}

func (m *Master) splitInput() {
	fi, err := os.Open(m.InputF)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()
	// make a read buffer
	r := bufio.NewReader(fi)
	numSlaves := len(m.Slaves)

	outf := make([]*bufio.Writer, numSlaves)
	for i := range m.Slaves {
		// open output file
		fo, err := os.Create(filepath.Join(m.MapDir, fmt.Sprintf("%d", i)))
		if err != nil {
			panic(err)
		}
		// close fo on exit and check for its returned error
		defer func(fout *os.File) {
			if err := fout.Close(); err != nil {
				panic(err)
			}
		}(fo)
		// make a write buffer
		outf[i] = bufio.NewWriter(fo)
	}
	count := 0
	for {
		c, err := r.ReadBytes(m.InputDelim)
		if err != nil && err != io.EOF {
			panic(err)
		}

		_, werr := outf[count%numSlaves].Write(c)

		if werr != nil {
			panic(err)
		}
		if err == io.EOF {
			break
		}
		count += 1
	}
	for _, fout := range outf {
		fout.Flush()
	}
}

func (m *Master) sendInputAndStartMaps() {
	for i, slave := range m.Slaves {
		srcpath := filepath.Join(m.MapDir, fmt.Sprintf("%d", i))
		destpath := getPath(m.MapDir)
		go func(host string, srcPath string, destPath string) {
			m.scpFile(srcPath, host, destPath)
			m.setPath(host, destPath)
			m.slaveMRCall(host, "Job.Map", &Args{})
			os.Remove(srcPath)
		}(slave, srcpath, destpath)
	}
	m.IdleSlaves = nil
}

func (m *Master) startReduce(slave string) {
	m.slaveMRCall(slave, "Job.Reduce", &Args{})
}

func (m *Master) decodeReduce(filename string) []task.ReduceCollector {
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
	return e
}

func (m *Master) concatReduceFiles() string {
	r1 := m.decodeReduce(filepath.Join(m.ReduceDir, "1"))
	r2 := m.decodeReduce(filepath.Join(m.ReduceDir, "2"))

	r := append(r1, r2...)
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(r)
	fn := getPath("/tmp")
	err := ioutil.WriteFile(fn, buf.Bytes(), 0600)
	if err != nil {
		panic(err)
	}
	os.Remove(filepath.Join(m.ReduceDir, "1"))
	os.Remove(filepath.Join(m.ReduceDir, "2"))
	return fn
}

func handleConnection(conn net.Conn, fschan chan Message) {
	dec := gob.NewDecoder(conn)

	var port string
	var length int
	dec.Decode(&port)
	dec.Decode(&length)
	r := make([]task.ReduceCollector, length)

	for i := 0; i < length; i++ {
		dec.Decode(&r[i])
	}

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(r)
	fn := getPath("/tmp")
	err := ioutil.WriteFile(fn, buf.Bytes(), 0600)
	if err != nil {
		panic(err)
	}
	ip := strings.Split(conn.RemoteAddr().String(), ":")[0]

	data := []string{fn, fmt.Sprintf("%s:%s", ip, port)}

	fschan <- Message{data, 1}
}
func (m *Master) FileServer(fschan chan Message) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", m.ReduceDataPort))
	if err != nil {
		// handle error
	}
	for {
		conn, err := ln.Accept() // this blocks until connection or error
		if err != nil {
			// handle error
			continue
		}
		go handleConnection(conn, fschan)
	}
}

func (m *Master) Controller() {
	os.RemoveAll(m.MapDir)
	os.RemoveAll(m.ReduceDir)
	os.Mkdir(m.MapDir, 0755)
	os.Mkdir(m.ReduceDir, 0755)

	fschan := make(chan Message)
	go m.FileServer(fschan)

	m.splitInput()

	m.sendInputAndStartMaps()
	copy(m.BusySlaves, m.Slaves)

	for {
		select {
		case fsmsg := <-fschan:
			if handleFS(fsmsg, m) { // We have got 2 files in reduce
				reduceFilepath := m.concatReduceFiles() //Also removes the files once its done
				slave := m.IdleSlaves[rand.Intn(len(m.IdleSlaves))]
				m.IdleSlaves = deleteFromArrayS(m.IdleSlaves, slave)
				m.BusySlaves = append(m.BusySlaves, slave)
				go func() {
					destpath := getPath(m.MapDir)
					m.scpFile(reduceFilepath, slave, destpath)
					m.setPath(slave, destpath)
					m.startReduce(slave)
					os.Remove(reduceFilepath)
				}()
			} else {
				if len(m.BusySlaves) == 0 {
					close(fschan)
					task.OutputWriter(m.decodeReduce(filepath.Join(m.ReduceDir, "1")))
					return
				}
			}
		}
	}

}

func handleFS(msg Message, m *Master) bool {
	if msg.code == 1 { // Slave completed Reduce
		m.IdleSlaves = append(m.IdleSlaves, msg.message[1])
		m.BusySlaves = deleteFromArrayS(m.BusySlaves, msg.message[1])
		filename := msg.message[0]
		if _, err := os.Stat(filepath.Join(m.ReduceDir, "1")); err == nil {
			os.Rename(filename, filepath.Join(m.ReduceDir, "2"))
			return true
		} else {
			os.Rename(filename, filepath.Join(m.ReduceDir, "1"))
			return false
		}
	}
	return false
}

type Message struct {
	message []string
	code    int
}

func findInArrayS(arr []string, item string) int {
	for i, elem := range arr {
		if elem == item {
			return i
		}
	}
	return -1
}

func deleteFromArrayS(arr []string, item string) []string {
	index := findInArrayS(arr, item)
	if index == -1 {
		return arr
	}
	return append(arr[:index], arr[index+1:]...)
}
func Run(configFile string) {
	rand.Seed(time.Now().Unix())
	config := configparser.ParseFile(configFile)
	master := NewMaster(config)
	master.Controller()
}
