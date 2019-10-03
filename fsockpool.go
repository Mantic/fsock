package fsock

import (
	"errors"
	"time"

	"github.com/sirupsen/logrus"
)

// Instantiates a new FSockPool
func NewFSockPool(maxFSocks int, fsaddr, fspasswd string, reconnects int, maxWaitConn time.Duration, eventHandlers map[string][]func(string, int), eventFilters map[string][]string, l logrus.FieldLogger, connIdx int) (*FSockPool, error) {
	pool := &FSockPool{
		connIdx:       connIdx,
		fsAddr:        fsaddr,
		fsPasswd:      fspasswd,
		reconnects:    reconnects,
		maxWaitConn:   maxWaitConn,
		eventHandlers: eventHandlers,
		eventFilters:  eventFilters,
		logger:        l,
		allowedConns:  make(chan struct{}, maxFSocks),
		fSocks:        make(chan *FSock, maxFSocks),
	}
	for i := 0; i < maxFSocks; i++ {
		pool.allowedConns <- struct{}{} // Empty initiate so we do not need to wait later when we pop
	}
	return pool, nil
}

// Connection handler for commands sent to FreeSWITCH
type FSockPool struct {
	connIdx       int
	fsAddr        string
	fsPasswd      string
	reconnects    int
	eventHandlers map[string][]func(string, int)
	eventFilters  map[string][]string
	logger        logrus.FieldLogger
	allowedConns  chan struct{} // Will be populated with members allowed
	fSocks        chan *FSock   // Keep here reference towards the list of opened sockets
	maxWaitConn   time.Duration // Maximum duration to wait for a connection to be returned by Pop
}

func (fsockpool *FSockPool) PopFSock() (fsock *FSock, err error) {
	if fsockpool == nil {
		return nil, errors.New("Unconfigured ConnectionPool")
	}
	if len(fsockpool.fSocks) != 0 { // Select directly if available, so we avoid randomness of selection
		fsock = <-fsockpool.fSocks
		return fsock, nil
	}
	select { // No fsock available in the pool, wait for first one showing up
	case fsock = <-fsockpool.fSocks:
	case <-fsockpool.allowedConns:
		fsock, err = NewFSock(fsockpool.fsAddr, fsockpool.fsPasswd, fsockpool.reconnects, fsockpool.eventHandlers, fsockpool.eventFilters, fsockpool.logger, fsockpool.connIdx)
		if err != nil {
			return nil, err
		}
		return fsock, nil
	case <-time.After(fsockpool.maxWaitConn):
		return nil, ErrConnectionPoolTimeout
	}
	return fsock, nil
}

func (fsockpool *FSockPool) PushFSock(fsk *FSock) {
	if fsockpool == nil { // Did not initialize the pool
		return
	}
	if fsk == nil || !fsk.Connected() {
		fsockpool.allowedConns <- struct{}{}
		return
	}
	fsockpool.fSocks <- fsk
}
