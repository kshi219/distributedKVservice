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
var numPeers int
var peerIPs map[int]string


var resources map[kvservice.Key]kvservice.Value // the k-v store


var elements map[kvservice.Key]*sync.RWMutex // map of locks for each key, pointers so we can wait on/modify
                                    // each individual lock without modifying the map

var resourceLock sync.RWMutex // we can only have one writer to the resources map
var elementsLock sync.RWMutex // we can only have one writer to the elements map
var txLock sync.Mutex

type Peer int

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
	resources = make(map[kvservice.Key]kvservice.Value)
	elements = make(map[kvservice.Key]*sync.RWMutex)
	global_txID = 0

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
	*reply = resources[key]
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



	for k,v := range mods.Writes {
		//write
		resourceLock.Lock()
		fmt.Println("write Value=(", v, ") to key=", "[", k, "]")
		resources[k] = v
		resourceLock.Unlock()

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