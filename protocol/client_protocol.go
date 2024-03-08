package protocol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
)

type Command string

const (
	CmdMsg Command = "MSG"
	CmdNop Command = "NOP"
	CmdBye Command = "BYE"
	CmdNil Command = "NIL"
)

type ConnectionPhase uint16

func (c ConnectionPhase) String() string {
	s, ok := phaseStrings[c]
	if ok {
		return s
	}
	return fmt.Sprintf("<Unknown ConnectionPhase %v>", int(c))
}

const (
	PhaseHandshake     ConnectionPhase = 1
	PhaseModeSelection ConnectionPhase = 2
)

var phaseStrings = map[ConnectionPhase]string{
	PhaseHandshake:     "Handshake",
	PhaseModeSelection: "ModeSelection",
}

type protocolError struct {
	phase   ConnectionPhase
	message string
}

func (p protocolError) Error() string {
	return fmt.Sprintf("protocol error, phase=%v, server message=%v", p.phase, p.message)
}

func NewClient(tx *bufio.Writer, rx *bufio.Reader) Client {
	return Client{
		tx: tx,
		rx: rx,
	}
}

type Client struct {
	tx *bufio.Writer
	rx *bufio.Reader
}

func (c *Client) read(v interface{}) error {
	return binary.Read(c.rx, binary.BigEndian, v)
}

// write given data in write format.
// Note: this method DOES NOT flush the write buffer!
func (c *Client) write(v interface{}) error {
	if v == nil {
		panic(errors.New("writing nil"))
	}
	vv := reflect.ValueOf(v)
	if vv.Kind() == reflect.String {
		_, err := c.tx.Write([]byte(vv.String()))
		return err
	}
	if vv.Kind() >= reflect.Int && vv.Kind() <= reflect.Uint64 {
		return binary.Write(c.tx, binary.BigEndian, v)
	}
	panic(fmt.Errorf("unsupported type to write: %v, kind: %v", vv.Type(), vv.Kind()))
}

func (c *Client) writeFlush(v interface{}) error {
	err := c.write(v)
	if err != nil {
		return err
	}
	return c.flush()
}

func (c *Client) flush() error {
	return c.tx.Flush()
}

const handshakeSequence = "PSMB"

func (c *Client) Handshake() error {
	err := c.write(handshakeSequence)
	if err != nil {
		return err
	}
	const (
		protocolVersion uint32 = 2
		protocolOptions uint32 = 0
	)
	err = c.write(protocolVersion)
	if err != nil {
		return err
	}
	err = c.write(protocolOptions)
	if err != nil {
		return err
	}
	err = c.flush()
	if err != nil {
		return err
	}
	msg, err := c.rx.ReadString('\x00')
	if err != nil {
		return err
	}
	msg = msg[:len(msg)-1]
	if msg != "OK" {
		return protocolError{
			phase:   PhaseHandshake,
			message: msg,
		}
	}
	var serverOptions uint32
	err = c.read(&serverOptions)
	if err != nil {
		return err
	}
	if serverOptions != 0 {
		return fmt.Errorf("invalid server options: %v", serverOptions)
	}
	return nil
}

func (c *Client) Publish(msg io.Reader, n int64) error {
	err := c.write(CmdMsg)
	if err != nil {
		return err
	}
	err = c.write(uint64(n))
	if err != nil {
		return err
	}
	_, err = io.CopyN(c.tx, msg, n)
	if err != nil {
		return err
	}
	err = c.flush()
	if err != nil {
		return err
	}
	return err
}

func (c *Client) PublishBytes(msg []byte) error {
	err := c.write(CmdMsg)
	if err != nil {
		return err
	}
	err = c.write(uint64(len(msg)))
	if err != nil {
		return err
	}
	err = c.write(msg)
	if err != nil {
		return err
	}
	return c.flush()
}

func (c *Client) Nop() error {
	return c.writeFlush(CmdNop)
}

func (c *Client) Bye() error {
	return c.writeFlush(CmdBye)
}

func (c *Client) Nil() error {
	return c.writeFlush(CmdNil)
}
