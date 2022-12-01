package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task struct {
	id        int
	File      string
	status    int
	startTime time.Time
	jobType   int
}
type HeartbeatMsg struct {
	reply *JobReply
	ok    chan struct{}
}
type ReportMsg struct {
	request *ReportArgs
	ok      chan struct{}
}
type Coordinator struct {
	// Your definitions here.
	phase       int // 0:map phase, 1: reduce phase
	nMap        int
	nReduce     int
	filenames   []string
	taskList    []Task
	heartbeatCh chan HeartbeatMsg
	reportCh    chan ReportMsg
	doneCh      chan struct{}
	exitCh      chan struct{}
	transform   chan struct{}
	finished    bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) JobHandler(args *JobArgs, reply *JobReply) error {
	// c.mu.Lock()
	// defer c.mu.Unlock()
	msg := HeartbeatMsg{reply, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}
func (c *Coordinator) ReportHandler(args *ReportArgs, reply *ReportReply) error {
	// c.mu.Lock()
	// defer c.mu.Unlock()
	msg := ReportMsg{args, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	// if args.JobType == 0 {
	// 	c.mapDone <- true
	// 	c.taskList[args.JobID].status = 2
	// } else {
	// 	c.reduceDone <- true
	// }
	return nil
}
func (c *Coordinator) schedule() {
	for {
		select {
		case <-c.exitCh:
			<-c.transform

		case msg := <-c.heartbeatCh:
			reply := msg.reply
			idx := -1
			for i, v := range c.taskList {
				if v.status == 0 || (v.status == 1 && time.Since(v.startTime) > time.Second*10) {
					idx = i
				}
			}
			if idx == -1 {
				reply.JobType = 3
				msg.ok <- struct{}{}
				break
			}
			task := c.taskList[idx]
			reply.File = task.File
			reply.JobType = c.phase
			reply.NReduce = c.nReduce
			reply.Nmap = c.nMap
			reply.JobID = task.id
			c.taskList[task.id].startTime = time.Now()
			c.taskList[task.id].status = 1
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			c.taskList[msg.request.JobID].status = 2
			c.doneCh <- struct{}{}
			msg.ok <- struct{}{}
		}

	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.

	return c.finished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.finished = false
	// Your code here.
	c.filenames = files
	for i := 0; i < len(files); i++ {
		t := Task{i, files[i], 0, time.Now(), 0}
		c.taskList = append(c.taskList, t)
	}
	fmt.Print(c.taskList)
	c.heartbeatCh = make(chan HeartbeatMsg)
	c.reportCh = make(chan ReportMsg)
	c.doneCh = make(chan struct{})
	c.exitCh = make(chan struct{})
	c.transform = make(chan struct{})
	go c.schedule()
	c.server()
	for i := 0; i < c.nMap; i++ {
		<-c.doneCh
	}
	fmt.Println("map phase done, exiting")
	c.exitCh <- struct{}{}
	c.phase = 1
	c.taskList = []Task{}
	for i := 0; i < c.nReduce; i++ {
		t := Task{}
		t.id = i
		t.jobType = c.phase
		c.taskList = append(c.taskList, t)
	}
	fmt.Print(c.taskList)
	c.transform <- struct{}{}
	for i := 0; i < c.nReduce; i++ {
		<-c.doneCh
	}
	fmt.Println("reduce phase done, exiting")

	c.exitCh <- struct{}{}
	c.phase = 2
	c.finished = true
	c.transform <- struct{}{}
	return &c
}
