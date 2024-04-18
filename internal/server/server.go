package server

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/willdot/messagebroker/internal"
)

// Action represents the type of action that a peer requests to do
type Action uint16

const (
	Subscribe   Action = 1
	Unsubscribe Action = 2
	Publish     Action = 3
	Ack         Action = 4
	Nack        Action = 5
)

// Status represents the status of a request
type Status uint16

const (
	Subscribed   Status = 1
	Unsubscribed Status = 2
	Error        Status = 3
)

func (s Status) String() string {
	switch s {
	case Subscribed:
		return "subscribed"
	case Unsubscribed:
		return "unsubscribed"
	case Error:
		return "error"
	}

	return ""
}

// StartAtType represents where the subcriber wishes to start subscribing to a topic from
type StartAtType uint16

const (
	Beginning StartAtType = 0
	Current   StartAtType = 1
	From      StartAtType = 2
)

// Server accepts subscribe and publish connections and passes messages around
type Server struct {
	Addr string
	lis  net.Listener

	mu     sync.Mutex
	topics map[string]*topic

	ackDelay   time.Duration
	ackTimeout time.Duration
}

// New creates and starts a new server
func New(Addr string, ackDelay, ackTimeout time.Duration) (*Server, error) {
	lis, err := net.Listen("tcp", Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	srv := &Server{
		lis:        lis,
		topics:     map[string]*topic{},
		ackDelay:   ackDelay,
		ackTimeout: ackTimeout,
	}

	slog.Info("starting server")
	go srv.start()

	return srv, nil
}

// Shutdown will cleanly shutdown the server
func (s *Server) Shutdown() error {
	return s.lis.Close()
}

func (s *Server) start() {
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				slog.Info("listener closed")
				return
			}
			slog.Error("listener failed to accept", "error", err)
			continue
		}

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	peer := NewPeer(conn)

	slog.Info("handling connection", "peer", peer.Addr())
	defer slog.Info("ending connection", "peer", peer.Addr())

	action, err := readAction(peer, 0)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			slog.Error("failed to read action from peer", "error", err, "peer", peer.Addr())
		}
		return
	}

	switch action {
	case Subscribe:
		s.handleSubscribe(peer)
	case Unsubscribe:
		s.handleUnsubscribe(peer)
	case Publish:
		s.handlePublish(peer)
	default:
		slog.Error("unknown action", "action", action, "peer", peer.Addr())
		writeInvalidAction(peer)
	}
}

func (s *Server) handleSubscribe(peer *Peer) {
	slog.Info("handling subscriber", "peer", peer.Addr())
	// subscribe the peer to the topic
	s.subscribePeerToTopic(peer)

	s.waitForPeerAction(peer)
}

func (s *Server) waitForPeerAction(peer *Peer) {
	// keep handling the peers connection, getting the action from the peer when it wishes to do something else.
	// once the peers connection ends, it will be unsubscribed from all topics and returned
	for {
		action, err := readAction(peer, time.Millisecond*100)
		if err != nil {
			// if the error is a timeout, it means the peer hasn't sent an action indicating it wishes to do something so sleep
			// for a little bit to allow for other actions to happen on the connection
			var neterr net.Error
			if errors.As(err, &neterr) && neterr.Timeout() {
				time.Sleep(time.Millisecond * 500)
				continue
			}

			if !errors.Is(err, io.EOF) {
				slog.Error("failed to read action from subscriber", "error", err, "peer", peer.Addr())
			}

			s.unsubscribePeerFromAllTopics(peer)

			return
		}

		switch action {
		case Subscribe:
			s.subscribePeerToTopic(peer)
		case Unsubscribe:
			s.handleUnsubscribe(peer)
		default:
			slog.Error("unknown action for subscriber", "action", action, "peer", peer.Addr())
			writeInvalidAction(peer)
			continue
		}
	}
}

func (s *Server) subscribePeerToTopic(peer *Peer) {
	op := func(conn net.Conn) error {
		// get the topics the peer wishes to subscribe to
		dataLen, err := dataLengthUint32(conn)
		if err != nil {
			slog.Error(err.Error(), "peer", peer.Addr())
			writeStatus(Error, "invalid data length of topics provided", conn)
			return nil
		}
		if dataLen == 0 {
			writeStatus(Error, "data length of topics is 0", conn)
			return nil
		}

		buf := make([]byte, dataLen)
		_, err = conn.Read(buf)
		if err != nil {
			slog.Error("failed to read subscibers topic data", "error", err, "peer", peer.Addr())
			writeStatus(Error, "failed to read topic data", conn)
			return nil
		}

		var topics []string
		err = json.Unmarshal(buf, &topics)
		if err != nil {
			slog.Error("failed to unmarshal subscibers topic data", "error", err, "peer", peer.Addr())
			writeStatus(Error, "invalid topic data provided", conn)
			return nil
		}

		var startAtType StartAtType
		err = binary.Read(conn, binary.BigEndian, &startAtType)
		if err != nil {
			slog.Error(err.Error(), "peer", peer.Addr())
			writeStatus(Error, "invalid start at type provided", conn)
			return nil
		}
		var startAt int
		switch startAtType {
		case From:
			var s uint16
			err = binary.Read(conn, binary.BigEndian, &s)
			if err != nil {
				slog.Error(err.Error(), "peer", peer.Addr())
				writeStatus(Error, "invalid start at value provided", conn)
				return nil
			}
			startAt = int(s)
		case Beginning:
			startAt = 0
		case Current:
			startAt = -1
		default:
			slog.Error("invalid start up type provided", "start up type", startAtType)
			writeStatus(Error, "invalid start up type provided", conn)
			return nil
		}

		s.subscribeToTopics(peer, topics, startAt)
		writeStatus(Subscribed, "", conn)

		return nil
	}

	_ = peer.RunConnOperation(op)
}

func (s *Server) handleUnsubscribe(peer *Peer) {
	slog.Info("handling unsubscriber", "peer", peer.Addr())
	op := func(conn net.Conn) error {
		// get the topics the peer wishes to unsubscribe from
		dataLen, err := dataLengthUint32(conn)
		if err != nil {
			slog.Error(err.Error(), "peer", peer.Addr())
			writeStatus(Error, "invalid data length of topics provided", conn)
			return nil
		}
		if dataLen == 0 {
			writeStatus(Error, "data length of topics is 0", conn)
			return nil
		}

		buf := make([]byte, dataLen)
		_, err = conn.Read(buf)
		if err != nil {
			slog.Error("failed to read subscibers topic data", "error", err, "peer", peer.Addr())
			writeStatus(Error, "failed to read topic data", conn)
			return nil
		}

		var topics []string
		err = json.Unmarshal(buf, &topics)
		if err != nil {
			slog.Error("failed to unmarshal subscibers topic data", "error", err, "peer", peer.Addr())
			writeStatus(Error, "invalid topic data provided", conn)
			return nil
		}

		s.unsubscribeToTopics(peer, topics)
		writeStatus(Unsubscribed, "", conn)

		return nil
	}

	_ = peer.RunConnOperation(op)
}

func (s *Server) handlePublish(peer *Peer) {
	slog.Info("handling publisher", "peer", peer.Addr())
	for {
		op := func(conn net.Conn) error {
			topicDataLen, err := dataLengthUint16(conn)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				slog.Error("failed to read data length", "error", err, "peer", peer.Addr())
				writeStatus(Error, "invalid data length of data provided", conn)
				return nil
			}
			if topicDataLen == 0 {
				return nil
			}
			topicBuf := make([]byte, topicDataLen)
			_, err = conn.Read(topicBuf)
			if err != nil {
				slog.Error("failed to read topic from peer", "error", err, "peer", peer.Addr())
				writeStatus(Error, "failed to read topic", conn)
				return nil
			}

			topicStr := string(topicBuf)
			if !strings.HasPrefix(topicStr, "topic:") {
				slog.Error("topic data does not contain topic prefix", "peer", peer.Addr())
				writeStatus(Error, "topic data does not contain 'topic:' prefix", conn)
				return nil
			}
			topicStr = strings.TrimPrefix(topicStr, "topic:")

			msgDataLen, err := dataLengthUint32(conn)
			if err != nil {
				slog.Error(err.Error(), "peer", peer.Addr())
				writeStatus(Error, "invalid data length of data provided", conn)
				return nil
			}
			if msgDataLen == 0 {
				return nil
			}

			dataBuf := make([]byte, msgDataLen)
			_, err = conn.Read(dataBuf)
			if err != nil {
				slog.Error("failed to read data from peer", "error", err, "peer", peer.Addr())
				writeStatus(Error, "failed to read data", conn)
				return nil
			}

			topic := s.getTopic(topicStr)
			if topic == nil {
				topic = newTopic(topicStr)
				s.topics[topicStr] = topic
			}

			message := internal.NewMessage(dataBuf)

			err = topic.sendMessageToSubscribers(message)
			if err != nil {
				slog.Error("failed to send message to subscribers", "error", err, "peer", peer.Addr())
				writeStatus(Error, "failed to send message to subscribers", conn)
				return nil
			}

			return nil
		}

		_ = peer.RunConnOperation(op)
	}
}

func (s *Server) subscribeToTopics(peer *Peer, topics []string, startAt int) {
	slog.Info("subscribing peer to topics", "topics", topics, "peer", peer.Addr())
	for _, topic := range topics {
		s.addSubsciberToTopic(topic, peer, startAt)
	}
}

func (s *Server) addSubsciberToTopic(topicName string, peer *Peer, startAt int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.topics[topicName]
	if !ok {
		t = newTopic(topicName)
	}

	t.mu.Lock()
	t.subscriptions[peer.Addr()] = newSubscriber(peer, t, s.ackDelay, s.ackTimeout, startAt)
	t.mu.Unlock()

	s.topics[topicName] = t
}

func (s *Server) unsubscribeToTopics(peer *Peer, topics []string) {
	slog.Info("unsubscribing peer from topics", "topics", topics, "peer", peer.Addr())
	for _, topic := range topics {
		s.removeSubsciberFromTopic(topic, peer)
	}
}

func (s *Server) removeSubsciberFromTopic(topicName string, peer *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.topics[topicName]
	if !ok {
		return
	}

	sub := t.findSubscription(peer.Addr())
	if sub == nil {
		return
	}

	sub.unsubscribe()
	t.removeSubscription(peer.Addr())
}

func (s *Server) unsubscribePeerFromAllTopics(peer *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, t := range s.topics {
		sub := t.findSubscription(peer.Addr())
		if sub == nil {
			return
		}

		sub.unsubscribe()
		t.removeSubscription(peer.Addr())
	}
}

func (s *Server) getTopic(topicName string) *topic {
	s.mu.Lock()
	defer s.mu.Unlock()

	if topic, ok := s.topics[topicName]; ok {
		return topic
	}

	return nil
}

func readAction(peer *Peer, timeout time.Duration) (Action, error) {
	var action Action
	op := func(conn net.Conn) error {
		if timeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
				slog.Error("failed to set connection read deadline", "error", err, "peer", peer.Addr())
			}
			defer func() {
				if err := conn.SetReadDeadline(time.Time{}); err != nil {
					slog.Error("failed to reset connection read deadline", "error", err, "peer", peer.Addr())
				}
			}()
		}

		err := binary.Read(conn, binary.BigEndian, &action)
		if err != nil {
			return err
		}
		return nil
	}

	err := peer.RunConnOperation(op)
	if err != nil {
		return 0, fmt.Errorf("failed to read action from peer: %w", err)
	}

	return action, nil
}

func writeInvalidAction(peer *Peer) {
	op := func(conn net.Conn) error {
		writeStatus(Error, "unknown action", conn)
		return nil
	}

	_ = peer.RunConnOperation(op)
}

func dataLengthUint32(conn net.Conn) (uint32, error) {
	var dataLen uint32
	err := binary.Read(conn, binary.BigEndian, &dataLen)
	if err != nil {
		return 0, err
	}
	return dataLen, nil
}

func dataLengthUint16(conn net.Conn) (uint16, error) {
	var dataLen uint16
	err := binary.Read(conn, binary.BigEndian, &dataLen)
	if err != nil {
		return 0, err
	}
	return dataLen, nil
}

func writeStatus(status Status, message string, conn net.Conn) {
	statusB := make([]byte, 2)
	binary.BigEndian.PutUint16(statusB, uint16(status))

	headers := statusB

	if len(message) > 0 {
		sizeB := make([]byte, 2)
		binary.BigEndian.PutUint16(sizeB, uint16(len(message)))
		headers = append(headers, sizeB...)
	}

	msgBytes := []byte(message)
	_, err := conn.Write(append(headers, msgBytes...))
	if err != nil {
		if !errors.Is(err, syscall.EPIPE) {
			slog.Error("failed to write status to peers connection", "error", err, "peer", conn.RemoteAddr())
		}
		return
	}
}
