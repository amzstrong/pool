package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Pool struct {
	MaxWorkerSize   int
	MaxJobQueueSize int
	GlobalTaskQueue chan *Task
	WorkerPool      chan *Worker
}

type Worker struct {
	Name        string
	TaskChannel chan *Task
	quit        chan bool
}

type Task struct {
	f    func() error
	Name string
}

func (t *Task) Execute() {
	t.f()
	log.Println(t.Name + " is over")
}

func (w *Worker) Start(p *Pool) {
	log.Println(w.Name + " starting ")
	go func() {
		for {
			p.WorkerPool <- w //注册到全局pool
			select {
			case task := <-w.TaskChannel:
				log.Println(w.Name + " start do task:" + task.Name)
				task.Execute()
			case <-w.quit:
				return
			}
		}
	}()
}
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

type Dispatcher struct {
	quit chan bool
	Pool *Pool
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.Pool.MaxWorkerSize; i++ {
		worker := &Worker{
			Name:        fmt.Sprintf("worker[%s] ", strconv.Itoa(i)),
			TaskChannel: make(chan *Task),
			quit:        make(chan bool),
		}
		worker.Start(d.Pool)
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case task := <-d.Pool.GlobalTaskQueue:
			log.Println("dispatcher get task:" + task.Name)
			time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
			go func(task *Task) {
				w := <-d.Pool.WorkerPool
				log.Println("dispatcher get worker:" + w.Name)
				w.TaskChannel <- task

			}(task)
		default:
		}

	}
}

func main() {
	outfile, err := os.OpenFile("Test.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666) //打开文件，若果文件不存在就创建一个同名文件并打开
	if err != nil {
		fmt.Println(*outfile, "open failed")
		os.Exit(1)
	}
	log.SetOutput(outfile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("start")

	d := Dispatcher{}
	d.quit = make(chan bool)
	d.Pool = &Pool{
		MaxWorkerSize:   2,
		MaxJobQueueSize: 5,
		GlobalTaskQueue: make(chan *Task),
		WorkerPool:      make(chan *Worker),
	}
	d.Run()

	i := 0
	for {
		p := &Task{
			f: func() error {
				log.Printf("task[%s] is doing", strconv.Itoa(i))
				return nil
			},
			Name: fmt.Sprintf("task[%s]", strconv.Itoa(i)),
		}
		d.Pool.GlobalTaskQueue <- p
		i++
		time.Sleep(time.Second)
		fmt.Println(i)
	}

}
