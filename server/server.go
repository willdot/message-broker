package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/willdot/messagebroker"
)

// Action represents the type of action that a peer requests to do
type Action uint8

const (
	Subscribe   Action = 1
	Unsubscribe Action = 2
	Publish     Action = 3
)

// Server accepts subscribe and publish connections and passes messages around
type Server struct {
	addr string
	lis  net.Listener

	mu     sync.Mutex
	topics map[string]topic
}

// New creates and starts a new server
func New(ctx context.Context, addr string) (*Server, error) {
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

// Shutdown will cleanly shutdown the server
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

func (s *Server) handleConn(conn net.Conn) {
	peer := newPeer(conn)
	action, err := peer.readAction()
	if err != nil {
		slog.Error("failed to read action from peer", "error", err, "peer", peer.addr())
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
		slog.Error("unknown action", "action", action, "peer", peer.addr())
		peer.writeStatus(Error, "unknown action")
		//_, _ = peer.Write([]byte("unknown action"))
	}
}

func (s *Server) handleSubscribe(peer peer) {
	// subscribe the peer to the topic
	s.subscribePeerToTopic(peer)

	// keep handling the peers connection, getting the action from the peer when it wishes to do something else.
	// once the peers connection ends, it will be unsubscribed from all topics and returned
	for {
		action, err := peer.readAction()
		if err != nil {
			// TODO: see if there's a way to check if the peers connection has been ended etc
			slog.Error("failed to read action from subscriber", "error", err, "peer", peer.addr())

			s.unsubscribePeerFromAllTopics(peer)

			return
		}

		switch action {
		case Subscribe:
			s.subscribePeerToTopic(peer)
		case Unsubscribe:
			s.handleUnsubscribe(peer)
		default:
			slog.Error("unknown action for subscriber", "action", action, "peer", peer.addr())
			continue
		}
	}
}

func (s *Server) subscribePeerToTopic(peer peer) {
	// get the topics the peer wishes to subscribe to
	dataLen, err := peer.readDataLength()
	if err != nil {
		slog.Error(err.Error(), "peer", peer.addr())
		peer.writeStatus(Error, "invalid data length of topics provided")
		// _, _ = peer.Write([]byte("invalid data length of topics provided"))
		return
	}
	if dataLen == 0 {
		peer.writeStatus(Error, "data length of topics is 0")
		// _, _ = peer.Write([]byte("data length of topics is 0"))
		return
	}

	buf := make([]byte, dataLen)
	_, err = peer.Read(buf)
	if err != nil {
		slog.Error("failed to read subscibers topic data", "error", err, "peer", peer.addr())
		peer.writeStatus(Error, "failed to read topic data")
		//_, _ = peer.Write([]byte("failed to read topic data"))
		return
	}

	var topics []string
	err = json.Unmarshal(buf, &topics)
	if err != nil {
		slog.Error("failed to unmarshal subscibers topic data", "error", err, "peer", peer.addr())
		peer.writeStatus(Error, "invalid topic data provided")
		//_, _ = peer.Write([]byte("invalid topic data provided"))
		return
	}

	s.subscribeToTopics(peer, topics)
	//_, _ = peer.Write([]byte("subscribed"))
	peer.writeStatus(Subscribed, "")
}

func (s *Server) handleUnsubscribe(peer peer) {
	// get the topics the peer wishes to unsubscribe from
	dataLen, err := peer.readDataLength()
	if err != nil {
		slog.Error(err.Error(), "peer", peer.addr())
		peer.writeStatus(Error, "invalid data length of topics provided")
		//_, _ = peer.Write([]byte("invalid data length of topics provided"))
		return
	}
	if dataLen == 0 {
		peer.writeStatus(Error, "data length of topics is 0")
		//_, _ = peer.Write([]byte("data length of topics is 0"))
		return
	}

	buf := make([]byte, dataLen)
	_, err = peer.Read(buf)
	if err != nil {
		slog.Error("failed to read subscibers topic data", "error", err, "peer", peer.addr())
		peer.writeStatus(Error, "failed to read topic data")
		//_, _ = peer.Write([]byte("failed to read topic data"))
		return
	}

	var topics []string
	err = json.Unmarshal(buf, &topics)
	if err != nil {
		slog.Error("failed to unmarshal subscibers topic data", "error", err, "peer", peer.addr())
		peer.writeStatus(Error, "invalid topic data provided")
		//_, _ = peer.Write([]byte("invalid topic data provided"))
		return
	}

	s.unsubscribeToTopics(peer, topics)
	peer.writeStatus(Unsubscribed, "")
	//_, _ = peer.Write([]byte("unsubscribed"))
}

func (s *Server) handlePublish(peer peer) {
	for {
		dataLen, err := peer.readDataLength()
		if err != nil {
			slog.Error(err.Error(), "peer", peer.addr())
			peer.writeStatus(Error, "invalid data length of data provided")
			//_, _ = peer.Write([]byte("invalid data length of data provided"))
			return
		}
		if dataLen == 0 {
			continue
		}

		buf := make([]byte, dataLen)
		_, err = peer.Read(buf)
		if err != nil {
			slog.Error("failed to read data from peer", "error", err, "peer", peer.addr())
			peer.writeStatus(Error, "failed to read data")
			//_, _ = peer.Write([]byte("failed to read data"))
			return
		}

		var msg messagebroker.Message
		err = json.Unmarshal(buf, &msg)
		if err != nil {
			peer.writeStatus(Error, "invalid message")
			//_, _ = peer.Write([]byte("invalid message"))
			slog.Error("failed to unmarshal data to message", "error", err, "peer", peer.addr())
			continue
		}

		topic := s.getTopic(msg.Topic)
		if topic != nil {
			topic.sendMessageToSubscribers(msg)
		}
	}
}

func (s *Server) subscribeToTopics(peer peer, topics []string) {
	for _, topic := range topics {
		s.addSubsciberToTopic(topic, peer)
	}
}

func (s *Server) addSubsciberToTopic(topicName string, peer peer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.topics[topicName]
	if !ok {
		t = newTopic(topicName)
	}

	t.subscriptions[peer.addr()] = subscriber{
		peer:          peer,
		currentOffset: 0,
	}

	s.topics[topicName] = t
}

func (s *Server) unsubscribeToTopics(peer peer, topics []string) {
	for _, topic := range topics {
		s.removeSubsciberFromTopic(topic, peer)
	}
}

func (s *Server) removeSubsciberFromTopic(topicName string, peer peer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.topics[topicName]
	if !ok {
		return
	}

	delete(t.subscriptions, peer.addr())
}

func (s *Server) unsubscribePeerFromAllTopics(peer peer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range s.topics {
		delete(topic.subscriptions, peer.addr())
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
