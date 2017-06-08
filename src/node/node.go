package main

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"net/http"
	"time"
)

var LOCALPID int

var peerIPs map[int]string

var rServerConn *rpc.Client


var resources map[string]string // the k-v store


var elements map[string]*sync.RWMutex // map of locks for each key, pointers so we can wait on/modify
                                    // each individual lock without modifying the map

var resourceLock sync.RWMutex // we can only have one writer to the resources map
var elementsLock sync.RWMutex // we can only have one writer to the elements map

type Peer int

type changes struct{
	writes map[string]string
	reads []string
}




func main() {

	resources = make(map[string]string)
	elements = make(map[string]*sync.RWMutex)

	pServer := rpc.NewServer()
	p := new(Peer)
	pServer.Register(p)

	l, err := net.Listen("tcp", peerIPs[LOCALPID])
	checkError("", err, true)
	for {
		conn, err := l.Accept()
		checkError("", err, false)
		go pServer.ServeConn(conn)
	}

	time.Sleep(600 * 1000 * time.Millisecond)
}

// called by Get, we assume all get requests correspond to existing keys
func (p *Peer) Read(key string, reply *string) error {
	// get elements rlock
	elementsLock.RLock()
	elock := elements[key]
	elementsLock.RUnlock()

	// readlock element
	(*elock).RLock()

	// read from element
	resourceLock.RLock()
	*reply = resources[key]
	resourceLock.RUnlock()
	return nil
}


// called by write, don't actually write anything, just lock for writing later during commit
func (p *Peer) Writelock(key string, reply *bool) error {
	// get element lock
	elementsLock.Lock()
	elock, exists := elements[key]

	if exists == false {
		elements[key] = new(sync.RWMutex)
		elock = elements[key]
	}

	elementsLock.Unlock()
	// write lock element
	(*elock).Lock()

	// read from element
	*reply = true
	return nil
}

// special write called by client who just read the same key
func (p *Peer) FreeReadThenWritelock(key string, reply *bool) error {
	// get element lock
	elementsLock.RLock()
	elock := elements[key]
	elementsLock.RUnlock()
	// write lock element
	(*elock).RUnlock()
	(*elock).Lock()

	// read from element
	*reply = true
	return nil
}

//
func (p *Peer) commit(mods changes, reply *bool) error {

	// write and release write locks
	resourceLock.Lock()
	for k,v := range mods.writes {
		//write
		resources[k] = v

		// get elements lock
		elementsLock.RLock()
		elock := elements[k]
		elementsLock.RUnlock()

		// release element
		(*elock).Unlock()

	}
	resourceLock.Unlock()

	// release read locks
	for _, k := range mods.reads {
		elementsLock.RLock()
		elock := elements[k]
		elementsLock.RUnlock()

		// release element
		(*elock).RUnlock()
	}




	*reply = true
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