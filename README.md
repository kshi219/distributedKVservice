# distributedKVservice
A distributed key-value store service implemented in Go. 


The goal of this project is to exercise distributed system concepts including concurrency control, failure recovery and replication

## Description
<img src="http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/assign6/arch.png" width="500">


This is a distributed key-value store service composed of N nodes. Each node will replicate the entire key-value store. Thus the system will be avaliable for up to N-1 node failures. This is service supports any number of concurrent clients on any node by using strict two phased locking. Replication among nodes is achieved using a optimistic model via a "Last-Write-Wins Timestamped Set" conflict-free replicated data type. Changes at each node are broadcast to all others on each commit and each client read yields to most recent write to the desired key out of all local as well as writes from remote nodes which have been delivered at the time of reading. Set growth is managed by trimming which happens during reads and receptions of commits broadcast from other nodes, at each trim, only the latest writes to each key are kept.



## Further Work
recovery using phased commits and persisted logs

clock drift management


 
#### acknowledgements
The starting base of this work is taken from the senior distributed systems [course](http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/) offered at my university. Due to scheduling issues I will not be able to take this class before graduation, but since the course materials are freely available online I decided to work through it on my own. This task is one of the final projects for the course. The CRDT bit was by my own design.
