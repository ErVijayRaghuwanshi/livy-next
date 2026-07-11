package session

import (
	"errors"
	"sync"
	"time"
)

// Manager handles the lifecycle of all interactive Spark sessions.
type Manager struct {
	mu          sync.RWMutex
	sessions    map[int]*Session
	nextID      int
	idleTimeout time.Duration
	stopCh      chan struct{}
}

// NewManager creates a new session Manager.
func NewManager(idleTimeout time.Duration) *Manager {
	m := &Manager{
		sessions:    make(map[int]*Session),
		nextID:      0,
		idleTimeout: idleTimeout,
		stopCh:      make(chan struct{}),
	}

	if idleTimeout > 0 {
		go m.cleanupLoop()
	}

	return m
}

// CreateSession allocates a new session ID and registers the session.
func (m *Manager) CreateSession(name string, kind string, client SparkClient) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := m.nextID
	m.nextID++

	sess := NewSession(id, name, kind, client)
	m.sessions[id] = sess
	return sess
}

// GetSession retrieves an active session by ID.
func (m *Manager) GetSession(id int) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sess, exists := m.sessions[id]
	return sess, exists
}

// ListSessions returns a slice of all registered sessions.
func (m *Manager) ListSessions() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		list = append(list, sess)
	}
	return list
}

// DeleteSession closes the session but does not immediately remove it from the manager, allowing stopped/dead sessions to be listed.
func (m *Manager) DeleteSession(id int) error {
	m.mu.Lock()
	sess, exists := m.sessions[id]
	if !exists {
		m.mu.Unlock()
		return errors.New("session not found")
	}
	m.mu.Unlock()

	return sess.Close()
}

// CloseAll closes all active sessions and stops the cleanup loop.
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.stopCh:
		// already closed
	default:
		close(m.stopCh)
	}

	for id, sess := range m.sessions {
		sess.Close()
		delete(m.sessions, id)
	}
}

func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.cleanupIdleSessions()
		}
	}
}

func (m *Manager) cleanupIdleSessions() {
	m.mu.Lock()
	var toDelete []int
	now := time.Now()

	for id, sess := range m.sessions {
		sess.mu.RLock()
		// Clean up starting, idle, or dead sessions that exceed the idle timeout
		shouldCleanup := sess.State == SessionIdle || sess.State == SessionStarting || sess.State == SessionDead
		if shouldCleanup && now.Sub(sess.LastActivity) > m.idleTimeout {
			toDelete = append(toDelete, id)
		}
		sess.mu.RUnlock()
	}

	for _, id := range toDelete {
		sess := m.sessions[id]
		delete(m.sessions, id)
		go sess.Close()
	}
	m.mu.Unlock()
}
