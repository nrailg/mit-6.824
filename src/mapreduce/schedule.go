package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.

	taskChan := make(chan int)
	go func() {
		for i := 0; i < ntasks; i++ {
			taskChan <- i
		}
		close(taskChan)
	}()

	doneChan := make(chan struct{})
	boss := func(workerAddr string) { // boss makes workers work :|
		for {
			taskI, ok := <-taskChan
			if !ok {
				break
			}
			args := DoTaskArgs {
				mr.jobName, mr.files[taskI], phase, taskI, nios,
			}
			_ = call(workerAddr, "Worker.DoTask", &args, new(struct{}))
			doneChan <- struct{}{}
		}
	}

	for _, wa := range mr.workers {
		go boss(wa)
	}

	nTaskDone := 0
	L:
	for {
		select {
		case wa := <-mr.registerChannel:
			go boss(wa)
		case <- doneChan:
			nTaskDone++
			if nTaskDone == ntasks {
				break L
			}
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
