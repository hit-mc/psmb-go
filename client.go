package psmb

import (
	"bufio"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/hit-mc/psmb-go/protocol"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Client provides auto reconnect and buffered abstraction for protocol.Client
type Client struct {
	conn                 *net.TCPConn
	impl                 protocol.Client // owned by keepSession
	mode                 protocol.ClientMode
	host                 string
	port                 uint16
	txQueue              chan OutboundMessage
	incompleteTx         []OutboundMessage // messages already taken out from txQueue but has not been sent
	closed               atomic.Bool       // closed is set only if Close is called.
	retryIntervalConnect time.Duration
	log                  logr.Logger
	errorConsumer        func(err error) // thread-safe
	heartBeatInterval    time.Duration   // zero means no heartbeat
	wgClose              sync.WaitGroup
}

func nopErrConsumer(error) {}

func NewClient(
	host string, port uint16,
	mode protocol.ClientMode,
	errorConsumer func(err error),
	heartBeatInterval time.Duration,
	connectRetryInterval time.Duration,
	logger logr.Logger,
) *Client {
	if errorConsumer == nil {
		errorConsumer = nopErrConsumer
	} else {
		realErrorConsumer := errorConsumer
		mu := &sync.Mutex{}
		errorConsumer = func(err error) {
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				return
			}
			realErrorConsumer(err)
		}
	}
	if heartBeatInterval < time.Millisecond*10 && heartBeatInterval != 0 {
		panic(fmt.Errorf("heartbeat interval is too short"))
	}
	c := &Client{
		impl:                 protocol.Client{},
		mode:                 mode,
		host:                 host,
		port:                 port,
		txQueue:              make(chan OutboundMessage, 128),
		incompleteTx:         make([]OutboundMessage, 0, 16),
		retryIntervalConnect: connectRetryInterval,
		log:                  logger,
		errorConsumer:        errorConsumer,
		heartBeatInterval:    heartBeatInterval,
	}
	go c.keepImpl()
	return c
}

// keepImpl ensures the impl is always alive and send/receive objects to/from the remote server.
func (c *Client) keepImpl() {
	for !c.closed.Load() {
		c.keepSession()
	}
}

// responder stores data that should be sent as a response.
type responder struct {
	nil        chan struct{} // nil is true if there is a NIL command to be sent.
	disconnect atomic.Bool   // disconnect is true if the remote peer has sent BYE or rx/tx has disconnected.
}

func (c *Client) send(re *responder) error {
	sendHeartBeat := true
	interval := c.heartBeatInterval
	if interval == 0 {
		interval = time.Duration(math.MaxInt64)
		sendHeartBeat = false
	}
	heartBeater := time.NewTicker(interval)
	defer heartBeater.Stop()
	for {
		closed := c.closed.Load()
		if closed {
			c.log.Info("client closed, break send loop")
			return nil
		}
		disconnected := re.disconnect.Load()
		if disconnected {
			c.log.Info("another direction disconnected, break send loop")
			return nil
		}
		select {
		case msg := <-c.txQueue:
			if c.mode.Type() != protocol.ModePublish {
				c.log.Error(nil, "cannot publish in non-publish mode, the message is ignored")
				continue
			}
			err := c.impl.Publish(msg.getContent())
			if err != nil {
				return fmt.Errorf("publish: %w", err)
			}
		case <-re.nil:
			// respond with NIL
			c.log.V(1).Info("sending NIL")
			err := c.impl.Nil()
			if err != nil {
				return fmt.Errorf("send nil: %w", err)
			}
		case <-heartBeater.C:
			if !sendHeartBeat {
				continue
			}
			err := c.impl.Nop()
			if err != nil {
				return fmt.Errorf("send nop: %w", err)
			}
		}
	}
}

func (c *Client) receive(re *responder) error {
	for {
		closed := c.closed.Load()
		if closed {
			c.log.Info("client closed, break receive loop")
			return nil
		}
		disconnected := re.disconnect.Load()
		if disconnected {
			c.log.Info("another direction disconnected, break receive loop")
			return nil
		}
		msg, err := c.impl.Receive()
		if err != nil {
			return err
		}
		msg.Consume(func(r io.Reader, length int64) {
			if c.mode.Type() != protocol.ModeSubscribe {
				c.log.Error(nil, "ignoring server data message in subscribe mode")
				return
			}
			c.mode.Consume(r, length)
		})
		cmd := msg.Command()
		c.log.V(1).Info("received message", "command", cmd)
		switch cmd {
		case protocol.CmdMsg:
			// ignore
		case protocol.CmdNop:
			select {
			case re.nil <- struct{}{}:
			default:
				c.log.Error(nil, "nil tx channel is full, sender thread may get blocked, "+
					"or server is sending NOP too fast")
			}
		case protocol.CmdBye:
			// we should stop this impl and reconnect
			re.disconnect.Store(true)
		case protocol.CmdNil:
			// ignore
		}
	}
}

func (c *Client) keepSession() {
	c.log.Info("connecting")
	closer, err := c.connect()
	if err != nil {
		c.errorConsumer(fmt.Errorf("failed to connect, waiting for reconnect: %w", err))
		time.Sleep(c.retryIntervalConnect)
		return
	}
	defer closer()
	c.log.V(1).Info("handshaking")
	err = c.impl.Handshake()
	if err != nil {
		c.errorConsumer(fmt.Errorf("handshake: %w", err))
		return
	}
	c.log.V(1).Info("selecting mode")
	err = c.impl.SelectMode(c.mode)
	if err != nil {
		c.errorConsumer(fmt.Errorf("select mode: %w", err))
		return
	}
	c.log.Info("session started")
	// tx thread
	rxTxSharedCtx := responder{
		nil: make(chan struct{}, 8),
	}
	if c.closed.Load() {
		return
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c.wgClose.Add(1)
	go func() {
		defer wg.Done()
		defer c.wgClose.Done()
		err := c.send(&rxTxSharedCtx)
		if err != nil {
			c.errorConsumer(fmt.Errorf("sender: %w", err))
		}
		c.log.Info("sender stopped")
	}()
	wg.Add(1)
	c.wgClose.Add(1)
	go func() {
		defer wg.Done()
		defer c.wgClose.Done()
		err := c.receive(&rxTxSharedCtx)
		if err != nil {
			c.errorConsumer(fmt.Errorf("receiver: %w", err))
		}
		c.log.Info("receiver stopped")
	}()
	wg.Wait()
	c.log.Info("session stopped")
}

// connect initiates the TCP connection to server and creates impl instance.
func (c *Client) connect() (closer func(), err error) {
	addr, err := net.ResolveTCPAddr("tcp", c.host+":"+strconv.Itoa(int(c.port)))
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("dial TCP: %w", err)
	}
	c.impl = protocol.NewClient(bufio.NewWriter(conn), bufio.NewReader(conn))
	return func() {
		_ = conn.Close()
	}, nil
}

func (c *Client) Close() error {
	if c.closed.Load() {
		return nil
	}
	c.closed.Store(true)
	c.wgClose.Wait()
	return nil
}
