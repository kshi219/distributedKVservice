package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import "./kvservice"

import "fmt"
import "time"


func p1(wait chan int){
	var nodes []string
	nodes = append(nodes, ":1234")



	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	//txID 0: put key and read twice at same key,
	// only first put should incur rpc call

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	fmt.Println("p1 start", time.Now())

	//success, err := t.Put("hello", "p1world")
	//fmt.Printf("Put returned: %v, %v\n", success, err)
	//
	//success, v, err := t.Get("hello")
	//fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)
	//
	//success, err = t.Put("hello", "p1world2")
	//fmt.Printf("Put returned: %v, %v\n", success, err)


	success, v, err := t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	wait <- 2

	time.Sleep(1 * 1000 * time.Millisecond)


	success, txID, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, at %v\n", success, txID, time.Now())

	c.Close()

}

func p2(wait chan int){
	var nodes []string
	nodes = append(nodes, ":1234")


	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	//txID 0: put key and read twice at same key,
	// only first put should incur rpc call

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	<-wait

	fmt.Println("p2 start", time.Now())
	success, err := t.Put("hello2", "p2world")
	fmt.Printf("Put returned: %v, %v\n", success, err)
	//
	//success, v, err := t.Get("hello2")
	//fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)
	//
	//success, err = t.Put("hello2", "p2world2")
	//fmt.Printf("Put returned: %v, %v\n", success, err)
	//
	//success, v, err := t.Get("hello")
	//fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, txID, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, at %v\n", success, txID, time.Now())

	c.Close()
}


func main() {



	var nodes []string
	nodes = append(nodes, ":1234")




	// txID 2: read so we latest write was successful

	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put("hello", "world")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, txID, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	c.Close()

	wait := make(chan int)

	go p1(wait)
	go p2(wait)


	time.Sleep(3 * 1000 * time.Millisecond)

	// txID 2: read so we latest write was successful

	c = kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err = c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, v, err := t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, v, err = t.Get("hello2")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success, txID, err = t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	c.Close()

}

