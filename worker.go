package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

type Worker struct {
	conn net.Conn
	name string
	s    *Server

	sync.Mutex
	asleep bool

	job *Job
}

func (w *Worker) handle(scanner *bufio.Scanner) error {
	for scanner.Scan() {
		p, err := PacketFromBytes(scanner.Bytes())
		if err != nil {
			return err
		}
		if err := w.handlePacket(p); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (w *Worker) sendPacket(p *Packet) error {
	b, err := p.Bytes()
	if err != nil {
		return err
	}
	_, err = io.Copy(w.conn, bytes.NewBuffer(b))
	return err
}

func (w *Worker) handlePacket(p Packet) error {
	switch p.Type {
	case PreSleep:
		w.Sleep()
		return nil
	case GrabJob:
		return nil
	case GrabJobUniq:
		j := w.s.jobs.Get(w.name)
		var resp *Packet
		if j == nil {
			resp = &Packet{Code: Res, Type: NoJob}
		} else {
			resp = &Packet{Code: Res, Type: JobAssignUniq}
			resp.SetArgs([][]byte{
				[]byte(j.handle), // Handle
				[]byte(w.name),   // Function
				[]byte{},         // Unique client ID
				j.payload,        // Payload
			})
			w.job = j
		}
		return w.sendPacket(resp)
	case WorkData, WorkWarning, WorkComplete, WorkFail:
		if w.job == nil {
			log.Printf("worker %s received packet %d when not working", w.name, p.Type)
			return nil
		}
		return w.job.client.sendPacket(p)
	default:
		return fmt.Errorf("unsupported or invalid type for worker %d", p.Type)
	}
	return fmt.Errorf("unreachable")
}

func (w *Worker) Sleep() {
	w.Lock()
	w.asleep = true
	w.Unlock()
}

func (w *Worker) Wake() (bool, error) {
	w.Lock()
	defer w.Unlock()
	if !w.asleep {
		return false, nil
	}
	if err := w.sendPacket(&Packet{Code: Res, Type: NoOp}); err != nil {
		return false, err
	}
	return true, nil
}
