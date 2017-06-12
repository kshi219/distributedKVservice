# distributedKVservice
A distributed key-value store service implemented in Go.

***WORK IN PROGRESS, COMMENCED ON JUNE 4, 2017*** 

The goal of this project is to exercise distributed system concepts including concurrency control, failure recovery and RPC's.

## Description
<img src="http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/assign6/arch.png" width="500">

This is to be a distributed key-value store service composed of N nodes. Each node will replicate the entire key-value store. Thus the system will be avaliable for up to N-1 node failures. By using strict 2-phase commit and write-ahead logging the service will provide transactional semantics to clients satisfying **A**tomic, **C**onsistent, **I**ndependent **D**urable semantic requirements. 

After a basic service is built, ideally this can serve as a base for exploring optimistic replication with Conflict-free Replicated Data Types and concurrency control backed by a block-chain protocol.



#### acknowledgements
The starting base of this work is taken from the senior distributed systems [course](http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/) offered at my university. Due to scheduling issues I will not be able to take this class before graduation, but since the course materials are freely available online I decided to work through it on my own. This task is one of the final projects for the course.
