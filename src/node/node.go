package node

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
	"../kvservice"
)

//var LOCALPID int
//
//var peerIPs map[int]string

//var rServerConn *rpc.Client


var resources map[kvservice.Key]kvservice.Value // the k-v store


var elements map[kvservice.Key]*sync.RWMutex // map of locks for each key, pointers so we can wait on/modify
                                    // each individual lock without modifying the map

var resourceLock sync.RWMutex // we can only have one writer to the resources map
var elementsLock sync.RWMutex // we can only have one writer to the elements map

type Peer int

type Changes struct{
	Writes map[kvservice.Key]kvservice.Value
	// for reads the value of the map is not used..
	// may be a better struct for this? TODO
	Reads map[kvservice.Key]kvservice.Value
}




func main() {

	resources = make(map[kvservice.Key]kvservice.Value)
	elements = make(map[kvservice.Key]*sync.RWMutex)

	pServer := rpc.NewServer()
	p := new(Peer)
	pServer.Register(p)

	l, err := net.Listen("tcp", ":1234")
	checkError("", err, true)
	for {
		conn, err := l.Accept()
		checkError("", err, false)
		go pServer.ServeConn(conn)
	}

	time.Sleep(600 * 1000 * time.Millisecond)
}

// called by Get, we assume all get requests correspond to existing keys
func (p *Peer) Read(key kvservice.Key, reply *kvservice.Value) error {
	// get elements rlock
	elementsLock.RLock()
	elock := elements[key]
	elementsLock.RUnlock()

	// read from element
	resourceLock.RLock()
	// readlock element
	(*elock).RLock()
	*reply = resources[key]
	resourceLock.RUnlock()
	return nil
}


// called by write, don't actually write anything, just lock for writing later during commit
func (p *Peer) Write(key kvservice.Key, reply *bool) error {
	// get element lock
	elementsLock.RLock()
	elock, exists := elements[key]
	elementsLock.RUnlock()

	if exists == false {
		elementsLock.Lock()
		elements[key] = new(sync.RWMutex)
		elock = elements[key]
		elementsLock.Unlock()
	}

	// write lock element
	(*elock).Lock()

	// read from element
	*reply = true
	return nil
}

// special write called by client who just read the same key
func (p *Peer) FreeReadThenWritelock(key kvservice.Key, reply *bool) error {
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

//commits a struct of changes, including writes which will happen "for real" now
//and reads which will drop their read locks
func (p *Peer) Commit(mods Changes, reply *bool) error {

	// write and release write locks
	resourceLock.Lock()
	for k,v := range mods.Writes {
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
	for k, _ := range mods.Reads {
		elementsLock.RLock()
		elock := elements[k]
		elementsLock.RUnlock()

		// release element
		(*elock).RUnlock()
	}




	*reply = true
	return nil
}

func (p *Peer) Abort(mods Changes, reply *bool) error {

	elementsLock.RLock()
	for k,_ := range mods.Writes {
		elock := elements[k]
		// release element
		(*elock).Unlock()

	}
	// release read locks
	for k, _ := range mods.Reads {
		elock := elements[k]
		// release element
		(*elock).RUnlock()
	}
	elementsLock.RUnlock()
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