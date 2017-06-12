package main

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
	"kvservice"
	"fmt"
	"strconv"
	"bufio"
	"strings"
	"io"
)

//var LOCALPID int
//
//var peerIPs map[int]string

//var rServerConn *rpc.Client

var LOCALPID int
var peerIPs map[int]string


var LWWsetlocal map[kvservice.Key][]ValueTime
var LWWsetpublic map[kvservice.Key][]ValueTime

type ValueTime struct {
	Value kvservice.Value
	ExeTime time.Time
}




var elements map[kvservice.Key]*sync.RWMutex // map of locks for each key, pointers so we can wait on/modify
                                    // each individual lock without modifying the map
var LWWsetlocalLock sync.Mutex
var LWWsetpublicLock sync.Mutex
var resourceLock sync.RWMutex // we can only have one writer to the resources map
var elementsLock sync.RWMutex // we can only have one writer to the elements map
var txLock sync.Mutex

type Peer int

type Commiter int

var global_txID int






func main() {
	args := os.Args[1:]

	// Missing command line args.
	if len(args) != 4 {
		fmt.Println("go run node.go [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]")
		return
	}

	myIDStr := args[1]
	myID, err := strconv.Atoi(myIDStr)
	checkError("", err, true)
	LOCALPID = myID
	clientFacingIP := args[3]
	pF, err := os.Open(args[0])

	peerIPs = make(map[int]string)
	bufIo := bufio.NewReader(pF)
	fmt.Println("parse file")
	for i := 1; ; i++ {
		peerIp, err := bufIo.ReadString('\n')
		peerIp = strings.TrimSpace(peerIp)
		if err == io.EOF {
			break
		}
		peerIPs[i] = peerIp
	}

	fmt.Println("finished setup")
	elements = make(map[kvservice.Key]*sync.RWMutex)
	LWWsetlocal = make(map[kvservice.Key][]ValueTime)
	LWWsetpublic = make(map[kvservice.Key][]ValueTime)
	global_txID = 0

	go receiveCommitsServer(peerIPs[LOCALPID])

	pServer := rpc.NewServer()
	p := new(Peer)
	pServer.Register(p)

	fmt.Println("starting service")

	l, err := net.Listen("tcp", clientFacingIP)
	checkError("", err, true)
	for {
		fmt.Println("waiting from connection")
		conn, err := l.Accept()
		checkError("", err, false)
		fmt.Println("got connection at: ", conn)
		go pServer.ServeConn(conn)
	}

	time.Sleep(60 * 1000 * time.Millisecond)
}


func receiveCommitsServer(externIP string) {
	rServer := rpc.NewServer()
	c := new(Commiter)
	rServer.Register(c)

	fmt.Println("starting recieve commit service")

	l, err := net.Listen("tcp", externIP)
	checkError("", err, true)
	for {
		fmt.Println("waiting from connection")
		conn, err := l.Accept()
		checkError("", err, false)
		fmt.Println("got connection at: ", conn)
		go rServer.ServeConn(conn)
	}
}

func (c *Commiter) ReceiveCommit(newSet map[kvservice.Key][]ValueTime, success *bool) error{
	LWWsetpublicLock.Lock()

	for k,vtslice := range newSet {
		//for each key in incoming set, take max of new set and existing set
		newTime := vtslice[0].ExeTime
		newval := vtslice[0].Value
		for _,vt := range vtslice {
			if vt.ExeTime.After(newTime) {
				newval = vt.Value
				newTime = vt.ExeTime
			}
		}

		oldTime := LWWsetpublic[k][0].ExeTime
		oldval := LWWsetpublic[k][0].Value
		for _,vt := range LWWsetpublic[k] {
			if vt.ExeTime.After(oldTime) {
				oldval = vt.Value
				oldTime = vt.ExeTime
			}
		}

		if oldTime.Before(newTime) {
			LWWsetpublic[k] = []ValueTime{ValueTime{newval, newTime}}
		} else {
			LWWsetpublic[k] = []ValueTime{ValueTime{oldval, oldTime}}
		}
	}

	LWWsetpublicLock.Unlock()
	return nil
}

func updategetlatestVal(key kvservice.Key) kvservice.Value{

	var ret kvservice.Value
	LWWsetpublicLock.Lock()
	publicvt := LWWsetpublic[key][0]
	LWWsetpublicLock.Unlock()

	LWWsetlocalLock.Lock()
	var lasttime time.Time
	var localval kvservice.Value
	for _,vt := range LWWsetlocal[key] {
		if vt.ExeTime.After(lasttime) {
			localval = vt.Value
			lasttime = vt.ExeTime
		}
	}
	if lasttime.After(publicvt.ExeTime){
		LWWsetlocal[key] = []ValueTime{ValueTime{localval,lasttime}}
		ret = localval
	} else {
		LWWsetlocal[key] = []ValueTime{ValueTime{publicvt.Value,publicvt.ExeTime}}
		ret = publicvt.Value
	}
	LWWsetlocalLock.Unlock()

	return ret

}

// called by Get, we assume all get requests correspond to existing keys
func (p *Peer) Read(key kvservice.Key, reply *kvservice.Value) error {
	// get elements rlock
	fmt.Println("read rpc called")
	elementsLock.RLock()
	elock := elements[key]
	elementsLock.RUnlock()


	// readlock element
	fmt.Println("try get read lock for key: [", key, "]")
	(*elock).RLock()
	fmt.Println("got read lock for key: [", key, "]")
	// read from element
	resourceLock.RLock()
	*reply = updategetlatestVal(key)
	resourceLock.RUnlock()
	return nil
}


// called by write, don't actually write anything, just lock for writing later during commit
func (p *Peer) Write(key kvservice.Key, reply *bool) error {
	// get ele	ment lock
	fmt.Println("write rpc called")
	elementsLock.RLock()
	elock, exists := elements[key]
	elementsLock.RUnlock()

	if exists == false {
		elementsLock.Lock()
		fmt.Println("[", key, "] is a new key, had to make new lock")
		elements[key] = new(sync.RWMutex)
		elock = elements[key]
		elementsLock.Unlock()
	}

	fmt.Println("try get write lock for key: [", key, "]")
	// write lock element
	(*elock).Lock()
	fmt.Println("got write lock for key: [", key, "]")


	// read from element
	*reply = true
	return nil
}

func (p *Peer) Record(kv kvservice.KeyValTime, reply *bool) error {
	//append to LWWset with write value and time
	LWWsetlocalLock.Lock()
	LWWsetlocal[kv.K] = append(LWWsetlocal[kv.K], ValueTime{kv.V, kv.T})
	LWWsetlocalLock.Unlock()
	*reply = true
	return nil
}

// special write called by client who just read the same key
func (p *Peer) FreeReadThenWritelock(key kvservice.Key, reply *bool) error {
	fmt.Println("write called on previously written key [", key, "]")
	// get element lock
	elementsLock.RLock()
	elock := elements[key]
	elementsLock.RUnlock()
	// write lock element
	fmt.Println("try free read lock and get write lock for key [", key, "]")
	(*elock).RUnlock()
	(*elock).Lock()
	fmt.Println("finished read lock and get write lock for key [", key, "]")

	// read from element
	*reply = true
	return nil
}

//commits a struct of changes, including writes which will happen "for real" now
//and reads which will drop their read locks
func (p *Peer) Commit(mods kvservice.Changes, txID *int) error {
	fmt.Println("commit called")
	txLock.Lock()
	*txID = global_txID
	global_txID += 1
	fmt.Println("txID determined to be ", *txID)
	txLock.Unlock()

	//broadcast Commit TODO make this more granular, broadcast individual puts
	for index, addr := range peerIPs {
		if index == LOCALPID {
			continue
		}
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return err
		}
		var success bool
		client.Call("Commiter.ReceiveCommit", LWWsetlocal, &success)

	}


	for k,_ := range mods.Writes {

		// get elements lock
		elementsLock.RLock()
		elock := elements[k]
		elementsLock.RUnlock()

		fmt.Println("release write lock at [", k, "]")
		// release element
		(*elock).Unlock()

	}


	// release read locks

	for k, _ := range mods.Reads {
		elementsLock.RLock()
		elock := elements[k]
		elementsLock.RUnlock()

		fmt.Println("release read lock at [", k, "]")
		// release element
		(*elock).RUnlock()
	}


	return nil
}

func (p *Peer) Abort(mods kvservice.Changes, success *bool) error {

	fmt.Println("Abort transaction")

	elementsLock.RLock()
	// its okay for this lock here to be so "big" since the lock operations inside
	// are all lock releases
	for k,_ := range mods.Writes {
		fmt.Println("release write lock at [", k, "]")
		elock := elements[k]
		// release element
		(*elock).Unlock()

	}
	// release read locks
	for k, _ := range mods.Reads {
		fmt.Println("release read lock at [", k, "]")
		elock := elements[k]
		// release element
		(*elock).RUnlock()
	}
	elementsLock.RUnlock()

	*success = true

	return nil

}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}