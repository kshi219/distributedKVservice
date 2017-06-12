package kvservice

import "fmt"
import "net/rpc"


// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string



type Changes struct{
	Writes map[Key]Value
	// for reads the value of the map is not used..
	// may be a better struct for this? TODO
	Reads map[Key]Value
}


// An interface representing a connection to the key-value store. To
// create a new connection use the NewConnection() method.
type connection interface {
	// The 'constructor' for a new logical transaction object. This is the
	// only way to create a new transaction. The returned transaction must
	// correspond to a specific, reachable, node in the k-v service. If
	// none of the nodes are reachable then tx must be nil and error must
	// be set (non-nil).
	NewTX() (newTX tx, err error)

	// Close the connection.
	Close()
}

// An interface representing a client's transaction. To create a new
// transaction use the connection.NewTX() method.
type tx interface {
	// Retrieves a value v associated with a key k as part of this
	// transaction. If success is true then v contains the value
	// associated with k and err is nil. If success is false then the
	// tx has aborted, v is an empty string, and err is non-nil. If
	// success is false, then all future calls on this transaction
	// must immediately return success = false (indicating an earlier
	// abort).
	Get(k Key) (success bool, v Value, err error)

	// Associates a value v with a key k as part of this
	// transaction. If success is true then put was recoded
	// successfully, otherwise the transaction has aborted (see
	// above).
	Put(k Key, v Value) (success bool, err error)

	// Commits this transaction. If success is true then commit
	// succeeded, otherwise the transaction has aborted (see above).
	// txID represents the transactions's global sequence number
	// (which determines this transaction's position in the serialized
	// sequence of all the other transactions executed by the
	// service).
	Commit() (success bool, txID int, err error)

	// Aborts this transaction. This call always succeeds.
	Abort()
}

//////////////////////////////////////////////

// The 'constructor' for a new logical connection object. This is the
// only way to create a new connection. Takes a set of k-v service
// node ip:port strings.
func NewConnection(nodes []string) connection {
	fmt.Printf("NewConnection\n")
	c := new(myconn)
	var err error
	c.client, err = rpc.Dial("tcp", nodes[0])
	if err != nil {
		return nil
	}
	return c
}

//////////////////////////////////////////////
// Connection interface

// Concrete implementation of a connection interface.
type myconn struct {
	client *rpc.Client
}

// Create a new transaction.
func (conn *myconn) NewTX() (tx, error) {
	fmt.Printf("NewTX\n")
	m := new(Mytx)
	m.client = conn.client
	m.changes = new(Changes)
	m.changes.Reads = make(map[Key]Value)
	m.changes.Writes = make(map[Key]Value)
	return m, nil
}

// Close the connection.
func (conn *myconn) Close() {
	fmt.Printf("Close\n")
	conn.client.Close()
}

// /Connection interface
//////////////////////////////////////////////

//////////////////////////////////////////////
// Transaction interface


// KEY ASSUMPTION, CLIENT WILL NOT GET/PUT CONCURRENTLY
// Concrete implementation of a tx interface.
type Mytx struct {
	changes *Changes
	client *rpc.Client
}

// Retrieves a value v associated with a key k.
func (t *Mytx) Get(k Key) (success bool, v Value, err error) {
	fmt.Printf("Get\n")
	//check if we are already have written to /read from this key
	if _, ok := t.changes.Writes[k]; ok {
		v = t.changes.Writes[k]
		return true, v, nil
	}

	if _, ok := t.changes.Reads[k]; ok {
		v = t.changes.Reads[k]
		return true, v, nil
	}


	err = t.client.Call("Peer.Read", k, &v)
	if err != nil {
		return false, "", err
	}
	t.changes.Reads[k] = v
	return true, v, nil
}

// Associates a value v with a key k.
func (t *Mytx) Put(k Key, v Value) (success bool, err error) {
	fmt.Printf("Put\n")
	// if we already have written to this value we do not need to get the lock again
	if _, ok := t.changes.Writes[k]; ok {
		t.changes.Writes[k] = v
		return true, nil
	}
	// we have not already written, so we must get lock
	// first we check if we have already read for this value,
	if _, ok := t.changes.Reads[k]; ok {
		err = t.client.Call("Peer.FreeReadThenWritelock", k, &success)
		if err != nil {
			return false, err
		}
		delete(t.changes.Reads,k)
		t.changes.Writes[k] = v
		return success, err
	} else {
		// we "write" by acquiring the write lock and saving the write value
		// to be commited later
		err = t.client.Call("Peer.Write", k, &success)
		if err != nil {
			return false, err
		}
		t.changes.Writes[k] = v
		return success, err
	}
}

// Commits the transaction.
func (t *Mytx) Commit() (success bool, txID int, err error) {
	fmt.Printf("Commit\n")
	// commit will write what needs to be written and drop all locks
	err = t.client.Call("Peer.Commit", t.changes, &txID)
	if err != nil {
		return false, 0, err
	}
	t.changes.Reads = make(map[Key]Value)
	t.changes.Writes = make(map[Key]Value)
	return true, txID, nil
}

// Aborts the transaction.
func (t *Mytx) Abort() {
	fmt.Printf("Abort\n")
	// this should abandon all write and drop all locks
	var success bool
	t.client.Call("Peer.Abort", t.changes, &success)
	// ditch changes
	t.changes.Reads = make(map[Key]Value)
	t.changes.Writes = make(map[Key]Value)

}

// /Transaction interface
//////////////////////////////////////////////