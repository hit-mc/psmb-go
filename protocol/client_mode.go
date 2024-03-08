package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

type ModeType uint16

const (
	ModePublish   ModeType = 1
	ModeSubscribe ModeType = 2
)

type ClientMode interface {
	Type() ModeType
	// Consume a message from remote peer. This method should panic if not supported.
	Consume(reader io.Reader, length int64)
	sendSelectionRequest(c *Client) error
}

func Publish(topicID string) ClientMode {
	return modePublish{
		topicID: topicID,
	}
}

type modePublish struct {
	topicID string
}

func (m modePublish) Consume(io.Reader, int64) {
	// it's the user's responsibility to filter out unsuitable messages
	panic(errors.New("remote peer should not send data message to publisher"))
}

func (m modePublish) Type() ModeType {
	return ModePublish
}

func (m modePublish) sendSelectionRequest(c *Client) error {
	if m.topicID == "" {
		panic(fmt.Errorf("empty subscription topic ID"))
	}
	return c.writeFlush("PUB" + m.topicID + "\x00")
}

func Subscribe(topicIDPattern string, messageConsumer func(reader io.Reader, length int64)) ClientMode {
	return modeSubscribe{
		topicIDPattern:  topicIDPattern,
		messageConsumer: messageConsumer,
	}
}

type modeSubscribe struct {
	topicIDPattern  string
	messageConsumer func(reader io.Reader, length int64)
}

func (m modeSubscribe) Consume(reader io.Reader, length int64) {
	m.messageConsumer(reader, length)
}

func (m modeSubscribe) Type() ModeType {
	return ModeSubscribe
}

func (m modeSubscribe) sendSelectionRequest(c *Client) error {
	return c.writeFlush("SUB\x00\x00\x00\x00" + m.topicIDPattern + "\x00")
}

func readModeSelectionResponse(r *bufio.Reader) error {
	msg, err := r.ReadString('\x00')
	if err != nil {
		return err
	}
	msg = msg[:len(msg)-1]
	if msg == "OK" {
		return nil
	}
	// failed or other protocol errors
	if msg == "FAILED" {
		msg2, err := r.ReadString('\x00')
		if err != nil {
			return err
		}
		msg += ": " + msg2[:len(msg2)-1]
	}
	return protocolError{
		phase:   PhaseModeSelection,
		message: msg,
	}
}

func (c *Client) SelectMode(mode ClientMode) error {
	err := mode.sendSelectionRequest(c)
	if err != nil {
		return err
	}
	return readModeSelectionResponse(c.rx)
}
