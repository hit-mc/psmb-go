package psmb

import (
	"bytes"
	"fmt"
	"github.com/hit-mc/psmb-go/protocol"
	"io"
)

// Publish a message. Panic if client mode is not Publisher. Block if send queue is full.
func (c *Client) Publish(msg []byte) error {
	if c.mode.Type() != protocol.ModePublish {
		panic(fmt.Errorf("invalid operation"))
	}
	c.txQueue <- bytesOutboundMessage{
		b:        msg,
		waitChan: make(chan error),
	}
	return nil
}

type OutboundMessage interface {
	// getContent return the content reader and length in bytes
	getContent() (io.Reader, int64)
	// Wait returns a channel which blocks until this message is successfully sent to the server.
	Wait() <-chan error
}

type bytesOutboundMessage struct {
	b        []byte
	waitChan chan error
}

func (b bytesOutboundMessage) getContent() (io.Reader, int64) {
	return bytes.NewReader(b.b), int64(len(b.b))
}

func (b bytesOutboundMessage) Wait() <-chan error {
	return b.waitChan
}
