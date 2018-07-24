package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// merge pipeline, availChan用来存储可用的worker，每个rpc调用结束之后，worker被重新推进availChan
	// 如下的逻辑是做了 registerChan 中worker（1. schedule之前已经存在的  2.schedule之后加入的）的merge
	availChan := make(chan string)
	done := make(chan bool)
	go func() {
		for {
			select {
				case worker := <-registerChan:
					availChan <- worker

				case <-done:
					break
			}
		}
	}()
	defer func(){
		done <- true
	}()


	switch phase {
	case mapPhase:
		doneTaskChan := make(chan int, ntasks)
		for taskIndex, mapFile := range mapFiles {
			worker := <- availChan
			mapFile := mapFile
			taskIndex := taskIndex
			doTaskArgs := DoTaskArgs{JobName:jobName, File:mapFile, Phase:phase, TaskNumber: taskIndex, NumOtherPhase:nReduce}
			go func() {
				// todo
				call(worker, "Worker.DoTask", doTaskArgs, nil)
				doneTaskChan <- taskIndex
				availChan <- worker
			}()
		}
		for _, _ = range mapFiles {
			<- doneTaskChan
		}
	case reducePhase:
		doneTaskChan := make(chan int, ntasks)
		for i := 0; i < nReduce; i++ {
			worker := <-availChan
			i := i
			doTaskArgs := DoTaskArgs{JobName:jobName, File:"", Phase:phase, TaskNumber: i, NumOtherPhase:len(mapFiles)}
			go func() {
				// todo
				call(worker, "Worker.DoTask", doTaskArgs, nil)
				doneTaskChan <- i
				availChan <- worker
			}()
		}
		for i := 0; i < nReduce; i++ {
			<- doneTaskChan
		}

	}
	fmt.Printf("Schedule: %v done\n", phase)
}
