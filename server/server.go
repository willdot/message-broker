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

	"github.com/willdot/messagebroker/server/peer"
)

// Action represents the type of action that a peer requests to do
type Action uint8

const (
	Subscribe   Action = 1
	Unsubscribe Action = 2
	Publish     Action = 3
)

// Status represents the status of a request
type Status uint8

const (
	Subscribed   = 1
	Unsubscribed = 2
	Error        = 3
)

func (s Status) String() string {
	switch s {
	case Subscribed:
		return "subsribed"
	case Unsubscribed:
		return "unsubscribed"
	case Error:
		return "error"
	}

	return ""
}

// Server accepts subscribe and publish connections and passes messages around
type Server struct {
	Addr string
	lis  net.Listener

	mu     sync.Mutex
	topics map[string]*topic
}

// New creates and starts a new server
func New(Addr string) (*Server, error) {
	lis, err := net.Listen("tcp", Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	srv := &Server{
		lis:    lis,
		topics: map[string]*topic{},
	}

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
	peer := peer.New(conn)

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

func (s *Server) handleSubscribe(peer *peer.Peer) {
	// subscribe the peer to the topic
	s.subscribePeerToTopic(peer)

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

func (s *Server) subscribePeerToTopic(peer *peer.Peer) {
	op := func(conn net.Conn) error {
		// get the topics the peer wishes to subscribe to
		dataLen, err := dataLength(conn)
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

		s.subscribeToTopics(peer, topics)
		writeStatus(Subscribed, "", conn)

		return nil
	}

	_ = peer.RunConnOperation(op)
}

func (s *Server) handleUnsubscribe(peer *peer.Peer) {
	op := func(conn net.Conn) error {
		// get the topics the peer wishes to unsubscribe from
		dataLen, err := dataLength(conn)
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

type messageToSend struct {
	topic string
	data  []byte
}

func (s *Server) handlePublish(peer *peer.Peer) {
	for {
		var message *messageToSend

		op := func(conn net.Conn) error {
			dataLen, err := dataLength(conn)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				slog.Error("failed to read data length", "error", err, "peer", peer.Addr())
				writeStatus(Error, "invalid data length of data provided", conn)
				return nil
			}
			if dataLen == 0 {
				return nil
			}
			topicBuf := make([]byte, dataLen)
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

			dataLen, err = dataLength(conn)
			if err != nil {
				slog.Error(err.Error(), "peer", peer.Addr())
				writeStatus(Error, "invalid data length of data provided", conn)
				return nil
			}
			if dataLen == 0 {
				return nil
			}

			dataBuf := make([]byte, dataLen)
			_, err = conn.Read(dataBuf)
			if err != nil {
				slog.Error("failed to read data from peer", "error", err, "peer", peer.Addr())
				writeStatus(Error, "failed to read data", conn)
				return nil
			}

			message = &messageToSend{
				topic: topicStr,
				data:  dataBuf,
			}
			return nil
		}

		_ = peer.RunConnOperation(op)

		if message == nil {
			continue
		}
		// TODO: this can be done in a go routine because once we've got the message from the publisher, the publisher
		// doesn't need to wait for us to send the message to all peers

		topic := s.getTopic(message.topic)
		if topic != nil {
			topic.sendMessageToSubscribers(message.data)
		}
	}
}

func (s *Server) subscribeToTopics(peer *peer.Peer, topics []string) {
	for _, topic := range topics {
		s.addSubsciberToTopic(topic, peer)
	}
}

func (s *Server) addSubsciberToTopic(topicName string, peer *peer.Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.topics[topicName]
	if !ok {
		t = newTopic(topicName)
	}

	t.subscriptions[peer.Addr()] = subscriber{
		peer:          peer,
		currentOffset: 0,
	}

	s.topics[topicName] = t
}

func (s *Server) unsubscribeToTopics(peer *peer.Peer, topics []string) {
	for _, topic := range topics {
		s.removeSubsciberFromTopic(topic, peer)
	}
}

func (s *Server) removeSubsciberFromTopic(topicName string, peer *peer.Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.topics[topicName]
	if !ok {
		return
	}

	delete(t.subscriptions, peer.Addr())
}

func (s *Server) unsubscribePeerFromAllTopics(peer *peer.Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range s.topics {
		delete(topic.subscriptions, peer.Addr())
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

func readAction(peer *peer.Peer, timeout time.Duration) (Action, error) {
	var action Action
	op := func(conn net.Conn) error {
		if timeout > 0 {
			err := conn.SetReadDeadline(time.Now().Add(timeout))
			if err != nil {
				slog.Error("failed to set connection read deadline", "error", err, "peer", peer.Addr())
			}
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

func writeInvalidAction(peer *peer.Peer) {
	op := func(conn net.Conn) error {
		writeStatus(Error, "unknown action", conn)
		return nil
	}

	_ = peer.RunConnOperation(op)
}

func dataLength(conn net.Conn) (uint32, error) {
	var dataLen uint32
	err := binary.Read(conn, binary.BigEndian, &dataLen)
	if err != nil {
		return 0, err
	}
	return dataLen, nil
}

func writeStatus(status Status, message string, conn net.Conn) {
	err := binary.Write(conn, binary.BigEndian, status)
	if err != nil {
		if !errors.Is(err, syscall.EPIPE) {
			slog.Error("failed to write status to peers connection", "error", err, "peer", conn.RemoteAddr())
		}
		return
	}

	if message == "" {
		return
	}

	msgBytes := []byte(message)
	err = binary.Write(conn, binary.BigEndian, uint32(len(msgBytes)))
	if err != nil {
		slog.Error("failed to write message length to peers connection", "error", err, "peer", conn.RemoteAddr())
		return
	}

	_, err = conn.Write(msgBytes)
	if err != nil {
		slog.Error("failed to write message to peers connection", "error", err, "peer", conn.RemoteAddr())
		return
	}
}
