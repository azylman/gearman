package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Code []byte

var (
	Req Code = []byte{0, 'R', 'E', 'Q'}
	Res Code = []byte{0, 'R', 'E', 'S'}
)

type Type int32

const (
	CanDo Type = iota + 1
	CantDo
	ResetAbilities
	PreSleep
	_
	NoOp
	SubmitJob
	JobCreated
	GrabJob
	NoJob
	JobAssign
	WorkStatus
	WorkComplete
	WorkFail
	GetStatus
	EchoReq
	EchoRes
	SubmitJobBg
	Error
	StatusRes
	SubmitJobHigh
	SetClientID
	CanDoTimeout
	AllYours
	WorkException
	OptionReq
	OptionRes
	WorkData
	WorkWarning
	GrabJobUniq
	JobAssignUniq
	SubmitJobHighBg
	SubmitJobLow
	SubmitJobLowBg
	SubmitJobSched
	SubmitJobEpoch
)

type Packet struct {
	Code
	Type
	Data []byte
}

func (p Packet) Bytes() ([]byte, error) {
	b := bytes.NewBuffer(p.Code)
	if err := binary.Write(b, binary.BigEndian, p.Type); err != nil {
		return nil, err
	}
	length := int32(0)
	if p.Data != nil {
		length = int32(len(p.Data))
	}
	if err := binary.Write(b, binary.BigEndian, length); err != nil {
		return nil, err
	}
	if p.Data != nil {
		if _, err := b.Write(p.Data); err != nil {
			return nil, err
		}
	}
	return b.Bytes(), nil
}

func (p *Packet) Args() [][]byte {
	return bytes.Split(p.Data, []byte{0})
}

func (p *Packet) SetArgs(args [][]byte) {
	p.Data = bytes.Join(args, []byte{0})
}

func PacketFromBytes(b []byte) (Packet, error) {
	p := Packet{}
	if bytes.Compare(b[:4], Req) == 0 {
		p.Code = Req
	} else if bytes.Compare(b[:4], Res) == 0 {
		p.Code = Res
	} else {
		return p, fmt.Errorf("invalid code %s", string(b[:4]))
	}

	var t int32
	if err := binary.Read(bytes.NewBuffer(b[4:8]), binary.BigEndian, &t); err != nil {
		return p, fmt.Errorf("invalid type %s", string(b[4:8]))
	}

	p.Type = Type(t)

	// headerSize = 12
	p.Data = make([]byte, len(b)-12)
	copy(p.Data, b[12:])
	return p, nil
}
