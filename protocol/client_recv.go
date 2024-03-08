package protocol

import (
	"bytes"
	"fmt"
	"io"
)

func (c *Client) Receive() (Message, error) {
	var cmd [3]byte
	_, err := io.ReadFull(c.rx, cmd[:])
	if err != nil {
		return nil, err
	}
	typ := Command(cmd[:])
	switch typ {
	case CmdMsg:
		var length uint64
		err = c.read(&length)
		if err != nil {
			return nil, err
		}
		data := make([]byte, length)
		_, err = io.ReadFull(c.rx, data)
		if err != nil {
			return nil, err
		}
		return commandMsg{
			data: data,
		}, nil
	case CmdNop:
		fallthrough
	case CmdBye:
		fallthrough
	case CmdNil:
		return trivialCommand(typ), nil
	default:
		return nil, fmt.Errorf("unknown command from server: %v", string(cmd[:]))
	}
}

type Message interface {
	Command() Command
	// Consume calls messageConsumer is this message contains a data message to the upper level.
	Consume(messageConsumer func(reader io.Reader, length int64))
}

type trivialCommand Command

func (t trivialCommand) Consume(func(reader io.Reader, length int64)) {}

func (t trivialCommand) Command() Command {
	return Command(t)
}

type commandMsg struct {
	data []byte
}

func (c commandMsg) Consume(messageConsumer func(reader io.Reader, length int64)) {
	messageConsumer(bytes.NewReader(c.data), int64(len(c.data)))
}

func (c commandMsg) Command() Command {
	return CmdMsg
}
