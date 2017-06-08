package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import "./kvservice"

import "fmt"

func main() {
	var nodes []string
	nodes = append(nodes, "alice:2001")
	nodes = append(nodes, "bob:2010")


	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put("hello", "world")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, v, err := t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, txID, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	c.Close()
}
