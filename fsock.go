/*
fsock.go is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM. All Rights Reserved.

Provides FreeSWITCH socket communication.

*/

package fsock

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	FS        *FSock // Used to share FS connection via package globals
	DelayFunc func() func() int

	ErrConnectionPoolTimeout = errors.New("ConnectionPool timeout")
)

func init() {
	DelayFunc = fib
}

// Connects to FS and starts buffering input
func NewFSock(fsaddr, fspaswd string, reconnects int, eventHandlers map[string][]func(string, int), eventFilters map[string][]string, l logrus.FieldLogger, connIdx int) (fsock *FSock, err error) {
	fsock = &FSock{
		fsMutex:         new(sync.RWMutex),
		connIdx:         connIdx,
		fsaddress:       fsaddr,
		fspaswd:         fspaswd,
		eventHandlers:   eventHandlers,
		eventFilters:    eventFilters,
		backgroundChans: make(map[string]chan string),
		cmdChan:         make(chan string),
		reconnects:      reconnects,
		delayFunc:       DelayFunc(),
		logger:          l,
	}
	if err = fsock.Connect(); err != nil {
		return nil, err
	}
	return
}

// Connection to FreeSWITCH Socket
type FSock struct {
	conn            net.Conn
	fsMutex         *sync.RWMutex
	connIdx         int // Indetifier for the component using this instance of FSock, optional
	buffer          *bufio.Reader
	fsaddress       string
	fspaswd         string
	eventHandlers   map[string][]func(string, int) // eventStr, connId
	eventFilters    map[string][]string
	backgroundChans map[string]chan string
	cmdChan         chan string
	reconnects      int
	delayFunc       func() int
	stopReadEvents  chan struct{} //Keep a reference towards forkedReadEvents so we can stop them whenever necessary
	errReadEvents   chan error
	logger          logrus.FieldLogger
}

func (fsock *FSock) connect() error {
	if fsock.Connected() {
		fsock.Disconnect()
	}

	conn, err := net.Dial("tcp", fsock.fsaddress)
	if err != nil {
		if fsock.logger != nil {
			fsock.logger.Errorf("<FSock> Attempt to connect to FreeSWITCH, received: %s", err.Error())
		}
		return err
	}
	fsock.fsMutex.Lock()
	fsock.conn = conn
	fsock.fsMutex.Unlock()
	if fsock.logger != nil {
		fsock.logger.Info("<FSock> Successfully connected to FreeSWITCH!")
	}
	// Connected, init buffer, auth and subscribe to desired events and filters
	fsock.fsMutex.RLock()
	fsock.buffer = bufio.NewReaderSize(fsock.conn, 8192) // reinit buffer
	fsock.fsMutex.RUnlock()

	if authChg, err := fsock.readHeaders(); err != nil || !strings.Contains(authChg, "auth/request") {
		return errors.New("No auth challenge received")
	} else if errAuth := fsock.auth(); errAuth != nil { // Auth did not succeed
		return errAuth
	}
	// Subscribe to events handled by event handlers
	if err := fsock.eventsPlain(getMapKeys(fsock.eventHandlers)); err != nil {
		return err
	}

	if err := fsock.filterEvents(fsock.eventFilters); err != nil {
		return err
	}
	go fsock.readEvents() // Fork read events in it's own goroutine
	return nil
}

func (fsock *FSock) send(cmd string) {
	fsock.fsMutex.RLock()
	// fmt.Fprint(fsock.conn, cmd)
	w, err := fsock.conn.Write([]byte(cmd))
	if err != nil {
		if fsock.logger != nil {
			fsock.logger.Errorf("<FSock> Cannot write command to socket <%s>", err.Error())
		}
		return
	}
	if w == 0 {
		if fsock.logger != nil {
			fsock.logger.Debug("<FSock> Cannot write command to socket: " + cmd)
		}
		return
	}
	fsock.fsMutex.RUnlock()
}

// Auth to FS
func (fsock *FSock) auth() error {
	fsock.send(fmt.Sprintf("auth %s\n\n", fsock.fspaswd))
	if rply, err := fsock.readHeaders(); err != nil {
		return err
	} else if !strings.Contains(rply, "Reply-Text: +OK accepted") {
		return fmt.Errorf("Unexpected auth reply received: <%s>", rply)
	}
	return nil
}

func (fsock *FSock) sendCmd(cmd string) (rply string, err error) {
	if err = fsock.ReconnectIfNeeded(); err != nil {
		return "", err
	}
	cmd = fmt.Sprintf("%s\n", cmd)
	fsock.send(cmd)
	rply = <-fsock.cmdChan
	if strings.Contains(rply, "-ERR") {
		return "", errors.New(strings.TrimSpace(rply))
	}
	return rply, nil
}

// Reads headers until delimiter reached
func (fsock *FSock) readHeaders() (header string, err error) {
	bytesRead := make([]byte, 0)
	var readLine []byte

	for {
		readLine, err = fsock.buffer.ReadBytes('\n')
		if err != nil {
			if fsock.logger != nil {
				fsock.logger.Errorf("<FSock> Error reading headers: <%s>", err.Error())
			}
			fsock.Disconnect()
			return "", err
		}
		// No Error, add received to localread buffer
		if len(bytes.TrimSpace(readLine)) == 0 {
			break
		}
		bytesRead = append(bytesRead, readLine...)
	}
	return string(bytesRead), nil
}

// Reads the body from buffer, ln is given by content-length of headers
func (fsock *FSock) readBody(noBytes int) (body string, err error) {
	bytesRead := make([]byte, noBytes)
	var readByte byte

	for i := 0; i < noBytes; i++ {
		if readByte, err = fsock.buffer.ReadByte(); err != nil {
			if fsock.logger != nil {
				fsock.logger.Errorf("<FSock> Error reading message body: <%s>", err.Error())
			}
			fsock.Disconnect()
			return "", err
		}
		// No Error, add received to local read buffer
		bytesRead[i] = readByte
	}
	return string(bytesRead), nil
}

// Event is made out of headers and body (if present)
func (fsock *FSock) readEvent() (header string, body string, err error) {
	var cl int

	if header, err = fsock.readHeaders(); err != nil {
		return "", "", err
	}
	if !strings.Contains(header, "Content-Length") { //No body
		return header, "", nil
	}
	if cl, err = strconv.Atoi(headerVal(header, "Content-Length")); err != nil {
		return "", "", errors.New("Cannot extract content length")
	}
	if body, err = fsock.readBody(cl); err != nil {
		return "", "", err
	}
	return
}

// Read events from network buffer, stop when exitChan is closed, report on errReadEvents on error and exit
// Receive exitChan and errReadEvents as parameters so we avoid concurrency on using fsock.
func (fsock *FSock) readEvents() {
	for {
		select {
		case <-fsock.stopReadEvents:
			return
		default: // Unlock waiting here
		}
		hdr, body, err := fsock.readEvent()
		if err != nil {
			fsock.errReadEvents <- err
			return
		}
		if strings.Contains(hdr, "api/response") {
			fsock.cmdChan <- body
		} else if strings.Contains(hdr, "command/reply") {
			fsock.cmdChan <- headerVal(hdr, "Reply-Text")
		} else if body != "" { // We got a body, could be event, try dispatching it
			fsock.dispatchEvent(body)
		}
	}
}

// Subscribe to events
func (fsock *FSock) eventsPlain(events []string) error {
	// if len(events) == 0 {
	// 	return nil
	// }
	eventsCmd := "event plain"
	customEvents := ""
	for _, ev := range events {
		if ev == "ALL" {
			eventsCmd = "event plain all"
			break
		}
		if strings.HasPrefix(ev, "CUSTOM") {
			customEvents += ev[6:] // will capture here also space between CUSTOM and event
			continue
		}
		eventsCmd += " " + ev
	}
	if eventsCmd != "event plain all" {
		eventsCmd += " BACKGROUND_JOB" // For bgapi
		if len(customEvents) != 0 {    // Add CUSTOM events subscribing in the end otherwise unexpected events are received
			eventsCmd += " " + "CUSTOM" + customEvents
		}
	}
	eventsCmd += "\n\n"
	fsock.send(eventsCmd)
	if rply, err := fsock.readHeaders(); err != nil {
		return err
	} else if !strings.Contains(rply, "Reply-Text: +OK") {
		fsock.Disconnect()
		return fmt.Errorf("Unexpected events-subscribe reply received: <%s>", rply)
	}
	return nil
}

// Enable filters
func (fsock *FSock) filterEvents(filters map[string][]string) error {
	if len(filters) == 0 {
		return nil
	}
	filters["Event-Name"] = append(filters["Event-Name"], "BACKGROUND_JOB") // for bgapi
	for hdr, vals := range filters {
		for _, val := range vals {
			cmd := "filter " + hdr + " " + val + "\n\n"
			fsock.send(cmd)
			if rply, err := fsock.readHeaders(); err != nil {
				return err
			} else if !strings.Contains(rply, "Reply-Text: +OK") {
				return fmt.Errorf("Unexpected filter-events reply received: <%s>", rply)
			}
		}
	}
	return nil
}

// Dispatch events to handlers in async mode
func (fsock *FSock) dispatchEvent(event string) {
	eventName := headerVal(event, "Event-Name")
	if eventName == "BACKGROUND_JOB" { // for bgapi BACKGROUND_JOB
		go fsock.doBackgroudJob(event)
		return
	}

	if eventName == "CUSTOM" {
		eventSubclass := headerVal(event, "Event-Subclass")
		if len(eventSubclass) != 0 {
			eventName += " " + urlDecode(eventSubclass)
		}
	}

	for _, handleName := range []string{eventName, "ALL"} {
		if _, hasHandlers := fsock.eventHandlers[handleName]; hasHandlers {
			// We have handlers, dispatch to all of them
			for _, handlerFunc := range fsock.eventHandlers[handleName] {
				go handlerFunc(event, fsock.connIdx)
			}
			return
		}
	}
	if fsock.logger != nil {
		fmt.Printf("No dispatcher, event name: %s, handlers: %+v\n", eventName, fsock.eventHandlers)
		fsock.logger.Warnf("<FSock> No dispatcher for event: <%+v>", event)
	}
}

// bgapi event lisen fuction
func (fsock *FSock) doBackgroudJob(event string) { // add mutex protection
	evMap := EventToMap(event)
	jobUUID, has := evMap["Job-UUID"]
	if !has {
		if fsock.logger != nil {
			fsock.logger.Errorf("<FSock> BACKGROUND_JOB with no Job-UUID")
		}
		return
	}

	var out chan string
	fsock.fsMutex.RLock()
	out, has = fsock.backgroundChans[jobUUID]
	fsock.fsMutex.RUnlock()
	if !has {
		if fsock.logger != nil {
			fsock.logger.Errorf("<FSock> BACKGROUND_JOB with UUID %s lost!", jobUUID)
		}
		return // not a requested bgapi
	}

	fsock.fsMutex.Lock()
	delete(fsock.backgroundChans, jobUUID)
	fsock.fsMutex.Unlock()

	out <- evMap[EventBodyTag]
}
