package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/azylman/gearman/scanner"
)

type Server struct {
	workers *Workers
	jobs    *Jobs
}

func NewServer() *Server {
	return &Server{
		workers: &Workers{workers: map[string][]*Worker{}},
		jobs:    &Jobs{jobs: map[string][]Job{}},
	}
}

func (s *Server) Listen(laddr string) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error retrieving hostname: %s", hostname)
	}
	s.jobs.hostname = hostname
	ln, err := net.Listen("tcp", laddr)
	if err != nil {
		return err
	}
	log.Printf("Listening on %s", laddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("error accepting connection: %s", err.Error())
			conn.Close()
			continue
		}
		go func() {
			defer conn.Close()
			if err := s.handle(conn); err != nil {
				log.Printf("error handling connection: %s", err.Error())
			}
		}()
	}
}

func (s *Server) handle(conn net.Conn) error {
	scanner := scanner.New(conn)

	if gotPacket := scanner.Scan(); !gotPacket {
		return scanner.Err()
	}
	p, err := PacketFromBytes(scanner.Bytes())
	if err != nil {
		return err
	}
	switch p.Type {
	case CanDo:
		name := string(p.Data)
		w := &Worker{conn: conn, name: name, s: s}
		s.workers.Add(name, w)
		return w.handle(scanner)
	case SubmitJob, SubmitJobBg:
		c := &Client{conn: conn, s: s}
		return c.handle(p, scanner)
	default:
		return fmt.Errorf("unable to infer connection type from packet type %d", p.Type)
	}
	return fmt.Errorf("unreachable")
}

func (s *Server) NewJob(name string, id, payload []byte, client *Client) string {
	s.jobs.Lock()
	if s.jobs.jobs[name] == nil {
		s.jobs.jobs[name] = []Job{}
	}
	handle := fmt.Sprintf("H:%s:%d", s.jobs.hostname, s.jobs.num)
	s.jobs.num++
	job := Job{client: client, id: id, payload: payload, handle: []byte(handle)}
	s.jobs.jobs[name] = append(s.jobs.jobs[name], job)
	s.jobs.Unlock()

	s.workers.Wake(name)
	return handle
}

type Workers struct {
	sync.Mutex
	workers map[string][]*Worker
}

func (w *Workers) Add(name string, worker *Worker) {
	w.Lock()
	if w.workers[name] == nil {
		w.workers[name] = []*Worker{}
	}
	w.workers[name] = append(w.workers[name], worker)
	w.Unlock()
}

func (w *Workers) Wake(name string) error {
	w.Lock()
	defer w.Unlock()
	if w.workers[name] == nil {
		return nil
	}
	for _, worker := range w.workers[name] {
		if awoken, err := worker.Wake(); err != nil {
			log.Printf("error waking %s worker: %s", name, err.Error())
		} else if awoken {
			break
		}
	}
	return nil
}

type Jobs struct {
	sync.Mutex
	jobs     map[string][]Job
	num      int
	hostname string
}

func (j *Jobs) Get(name string) *Job {
	j.Lock()
	defer j.Unlock()
	if j.jobs[name] == nil || len(j.jobs[name]) == 0 {
		return nil
	}
	var job Job
	job, j.jobs[name] = j.jobs[name][len(j.jobs[name])-1], j.jobs[name][:len(j.jobs[name])-1]
	return &job
}

type Job struct {
	payload []byte
	client  *Client
	handle  []byte
	id      []byte
}
