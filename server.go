package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
)

// Action represents the type of action that a connection requests to do
type Action uint8

const (
	Subscribe   Action = 1
	Unsubscribe Action = 2
	Publish     Action = 3
)

type message struct {
	Topic string `json:"topic"`
	Data  []byte `json:"data"`
}

type Server struct {
	addr string
	lis  net.Listener

	mu     sync.Mutex
	topics map[string]topic
}

func NewServer(ctx context.Context, addr string) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	srv := &Server{
		lis:    lis,
		topics: map[string]topic{},
	}

	go srv.start(ctx)

	return srv, nil
}

func (s *Server) Shutdown() error {
	return s.lis.Close()
}

func (s *Server) start(ctx context.Context) {
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			slog.Error("listener failed to accept", "error", err)
			// TODO: see if there's a better way to check for this error
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
		}

		go s.handleConn(conn)
	}
}

func getActionFromConn(conn net.Conn) (Action, error) {
	var action Action
	err := binary.Read(conn, binary.BigEndian, &action)
	if err != nil {
		return 0, err
	}

	return action, nil
}

func getDataLengthFromConn(conn net.Conn) (uint32, error) {
	var dataLen uint32
	err := binary.Read(conn, binary.BigEndian, &dataLen)
	if err != nil {
		return 0, fmt.Errorf("failed to read data length from conn: %w", err)
	}

	return dataLen, nil
}

func (s *Server) handleConn(conn net.Conn) {
	action, err := getActionFromConn(conn)
	if err != nil {
		slog.Error("failed to read action from conn", "error", err, "conn", conn.LocalAddr())
		return
	}

	switch action {
	case Subscribe:
		s.handleSubscribingConn(conn)
	case Unsubscribe:
		s.handleUnsubscribingConn(conn)
	case Publish:
		s.handlePublisherConn(conn)
	default:
		slog.Error("unknown action", "action", action, "conn", conn.LocalAddr())
		_, _ = conn.Write([]byte("unknown action"))
	}
}

func (s *Server) handleSubscribingConn(conn net.Conn) {
	// subscribe the connection to the topic
	s.subscribeConnToTopic(conn)

	// keep handling the connection, getting the action from the conection when it wishes to do something else.
	// once the connection ends, it will be unsubscribed from all topics and returned
	for {
		action, err := getActionFromConn(conn)
		if err != nil {
			// TODO: see if there's a way to check if the connection has been ended etc
			slog.Error("failed to read action from subscriber", "error", err, "conn", conn.LocalAddr())

			s.unsubscribeConnectionFromAllTopics(conn.LocalAddr())

			return
		}

		switch action {
		case Subscribe:
			s.subscribeConnToTopic(conn)
		case Unsubscribe:
			s.handleUnsubscribingConn(conn)
		default:
			slog.Error("unknown action for subscriber", "action", action, "conn", conn.LocalAddr())
			continue
		}
	}
}

func (s *Server) subscribeConnToTopic(conn net.Conn) {
	// get the topics the connection wishes to subscribe to
	dataLen, err := getDataLengthFromConn(conn)
	if err != nil {
		slog.Error(err.Error(), "conn", conn.LocalAddr())
		_, _ = conn.Write([]byte("invalid data length of topics provided"))
		return
	}
	if dataLen == 0 {
		_, _ = conn.Write([]byte("data length of topics is 0"))
		return
	}

	buf := make([]byte, dataLen)
	_, err = conn.Read(buf)
	if err != nil {
		slog.Error("failed to read subscibers topic data", "error", err, "conn", conn.LocalAddr())
		_, _ = conn.Write([]byte("failed to read topic data"))
		return
	}

	var topics []string
	err = json.Unmarshal(buf, &topics)
	if err != nil {
		slog.Error("failed to unmarshal subscibers topic data", "error", err, "conn", conn.LocalAddr())
		_, _ = conn.Write([]byte("invalid topic data provided"))
		return
	}

	s.subscribeToTopics(conn, topics)
	_, _ = conn.Write([]byte("subscribed"))
}

func (s *Server) handleUnsubscribingConn(conn net.Conn) {
	// get the topics the connection wishes to unsubscribe from
	dataLen, err := getDataLengthFromConn(conn)
	if err != nil {
		slog.Error(err.Error(), "conn", conn.LocalAddr())
		_, _ = conn.Write([]byte("invalid data length of topics provided"))
		return
	}
	if dataLen == 0 {
		_, _ = conn.Write([]byte("data length of topics is 0"))
		return
	}

	buf := make([]byte, dataLen)
	_, err = conn.Read(buf)
	if err != nil {
		slog.Error("failed to read subscibers topic data", "error", err, "conn", conn.LocalAddr())
		_, _ = conn.Write([]byte("failed to read topic data"))
		return
	}

	var topics []string
	err = json.Unmarshal(buf, &topics)
	if err != nil {
		slog.Error("failed to unmarshal subscibers topic data", "error", err, "conn", conn.LocalAddr())
		_, _ = conn.Write([]byte("invalid topic data provided"))
		return
	}

	s.unsubscribeToTopics(conn, topics)

	_, _ = conn.Write([]byte("unsubscribed"))
}

func (s *Server) handlePublisherConn(conn net.Conn) {
	dataLen, err := getDataLengthFromConn(conn)
	if err != nil {
		slog.Error(err.Error(), "conn", conn.LocalAddr())
		_, _ = conn.Write([]byte("invalid data length of data provided"))
		return
	}
	if dataLen == 0 {
		return
	}

	buf := make([]byte, dataLen)
	_, err = conn.Read(buf)
	if err != nil {
		_, _ = conn.Write([]byte("failed to read data"))
		slog.Error("failed to read data from conn", "error", err, "conn", conn.LocalAddr())
		return
	}

	var msg message
	err = json.Unmarshal(buf, &msg)
	if err != nil {
		_, _ = conn.Write([]byte("invalid message"))
		slog.Error("failed to unmarshal data to message", "error", err, "conn", conn.LocalAddr())
		return
	}

	topic := s.getTopic(msg.Topic)
	if topic != nil {
		topic.sendMessageToSubscribers(msg)
	}
}

func (s *Server) subscribeToTopics(conn net.Conn, topics []string) {
	for _, topic := range topics {
		s.addSubsciberToTopic(topic, conn)
	}
}

func (s *Server) addSubsciberToTopic(topicName string, conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.topics[topicName]
	if !ok {
		t = newTopic(topicName)
	}

	t.subscriptions[conn.LocalAddr()] = Subscriber{
		conn:          conn,
		currentOffset: 0,
	}

	s.topics[topicName] = t
}

func (s *Server) unsubscribeToTopics(conn net.Conn, topics []string) {
	for _, topic := range topics {
		s.removeSubsciberFromTopic(topic, conn)
	}
}

func (s *Server) removeSubsciberFromTopic(topicName string, conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.topics[topicName]
	if !ok {
		return
	}

	delete(t.subscriptions, conn.LocalAddr())
}

func (s *Server) unsubscribeConnectionFromAllTopics(addr net.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range s.topics {
		delete(topic.subscriptions, addr)
	}
}

func (s *Server) getTopic(topicName string) *topic {
	s.mu.Lock()
	defer s.mu.Unlock()

	if topic, ok := s.topics[topicName]; ok {
		return &topic
	}

	return nil
}
