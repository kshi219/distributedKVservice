

package kvservice

import "fmt"

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string


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
	return c
}

//////////////////////////////////////////////
// Connection interface

// Concrete implementation of a connection interface.
type myconn struct {
	// TODO
}

// Create a new transaction.
func (conn *myconn) NewTX() (tx, error) {
	fmt.Printf("NewTX\n")
	m := new(mytx)
	return m, nil
}

// Close the connection.
func (conn *myconn) Close() {
	fmt.Printf("Close\n")
	// TODO
}

// /Connection interface
//////////////////////////////////////////////

//////////////////////////////////////////////
// Transaction interface

// Concrete implementation of a tx interface.
type mytx struct {
	// TODO
}

// Retrieves a value v associated with a key k.
func (t *mytx) Get(k Key) (success bool, v Value, err error) {
	fmt.Printf("Get\n")
	// TODO
	return true, "hello", nil
}

// Associates a value v with a key k.
func (t *mytx) Put(k Key, v Value) (success bool, err error) {
	fmt.Printf("Put\n")
	// TODO
	return true, nil
}

// Commits the transaction.
func (t *mytx) Commit() (success bool, txID int, err error) {
	fmt.Printf("Commit\n")
	// TODO
	return true, 0, nil
}

// Aborts the transaction.
func (t *mytx) Abort() {
	fmt.Printf("Abort\n")
	// TODO
}

// /Transaction interface
//////////////////////////////////////////////