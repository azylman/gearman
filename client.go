package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
)

type Client struct {
	conn net.Conn
	s    *Server
}

func (c *Client) handle(first Packet, scanner *bufio.Scanner) error {
	if err := c.handlePacket(first); err != nil {
		return err
	}
	for scanner.Scan() {
		p, err := PacketFromBytes(scanner.Bytes())
		if err != nil {
			return err
		}
		if err := c.handlePacket(p); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (c *Client) sendPacket(p Packet) error {
	b, err := p.Bytes()
	if err != nil {
		return err
	}
	_, err = io.Copy(c.conn, bytes.NewBuffer(b))
	return err
}

func (c *Client) handlePacket(p Packet) error {
	switch p.Type {
	case SubmitJob:
		args := p.Args()
		handle := c.s.NewJob(string(args[0]), args[1], args[2], c)
		return c.sendPacket(Packet{
			Code: Res,
			Type: JobCreated,
			Data: []byte(handle),
		})
	case SubmitJobBg:
		return nil
	default:
		return fmt.Errorf("unsupported or invalid type for client %d", p.Type)
	}
	return fmt.Errorf("unreachable")
}
