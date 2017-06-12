package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import "./kvservice"

import "fmt"



func main() {
	var nodes []string
	nodes = append(nodes, ":1234")


	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	//txID 0: put key and read twice at same key,
	// only first put should incur rpc call

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put("hello", "world")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, v, err := t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, err = t.Put("hello", "world2")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, v, err = t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, txID, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	//txID 1: get twice, only first should incur RPC call,
	// then write to same key, which should invoke special RPC call
	// to release read lock then write

	t, err = c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, v, err = t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, v, err = t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, err = t.Put("hello", "Messi Tax Dodger")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, txID, err = t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	c.Close()

	// txID 2: read so we latest write was successful

	c = kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err = c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, v, err = t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, txID, err = t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	c.Close()
}
