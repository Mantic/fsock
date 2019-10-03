package fsock

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// Connect or reconnect
func (fsock *FSock) Connect() error {
	if fsock.stopReadEvents != nil {
		close(fsock.stopReadEvents) // we have read events already processing, request stop
	}
	// Reinit readEvents channels so we avoid concurrency issues between goroutines
	fsock.stopReadEvents = make(chan struct{})
	fsock.errReadEvents = make(chan error)
	return fsock.connect()
}

// Connected checks if socket connected. Can be extended with pings
func (fsock *FSock) Connected() (ok bool) {
	fsock.fsMutex.RLock()
	ok = (fsock.conn != nil)
	fsock.fsMutex.RUnlock()
	return
}

// Disconnects from socket
func (fsock *FSock) Disconnect() (err error) {
	fsock.fsMutex.Lock()
	if fsock.conn != nil {
		if fsock.logger != nil {
			fsock.logger.Info("<FSock> Disconnecting from FreeSWITCH!")
		}
		err = fsock.conn.Close()
		fsock.conn = nil
	}
	fsock.fsMutex.Unlock()
	return
}

// If not connected, attempt reconnect if allowed
func (fsock *FSock) ReconnectIfNeeded() (err error) {
	if fsock.Connected() { // No need to reconnect
		return nil
	}
	for i := 0; fsock.reconnects == -1 || i < fsock.reconnects; i++ { // Maximum reconnects reached, -1 for infinite reconnects
		if err = fsock.connect(); err == nil && fsock.Connected() {
			fsock.delayFunc = DelayFunc() // Reset the reconnect delay
			break                         // No error or unrelated to connection
		}
		time.Sleep(time.Duration(fsock.delayFunc()) * time.Second)
	}
	if err == nil && !fsock.Connected() {
		return errors.New("Not connected to FreeSWITCH")
	}
	return err // nil or last error in the loop
}

// Generic proxy for commands
func (fsock *FSock) SendCmd(cmdStr string) (string, error) {
	return fsock.sendCmd(cmdStr + "\n")
}

func (fsock *FSock) SendCmdWithArgs(cmd string, args map[string]string, body string) (string, error) {
	for k, v := range args {
		cmd += fmt.Sprintf("%s: %s\n", k, v)
	}
	if len(body) != 0 {
		cmd += fmt.Sprintf("\n%s\n", body)
	}
	return fsock.sendCmd(cmd)
}

// Send API command
func (fsock *FSock) SendAPICmd(cmdStr string) (string, error) {
	return fsock.sendCmd("api " + cmdStr + "\n")
}

// Send BGAPI command
func (fsock *FSock) SendBGAPICmd(cmdStr string) (out chan string, err error) {
	jobUUID := genUUID()
	out = make(chan string)

	fsock.fsMutex.Lock()
	fsock.backgroundChans[jobUUID] = out
	fsock.fsMutex.Unlock()

	_, err = fsock.sendCmd(fmt.Sprintf("bgapi %s\nJob-UUID:%s\n", cmdStr, jobUUID))
	if err != nil {
		return nil, err
	}
	return
}

// SendMsgCmdWithBody command
func (fsock *FSock) SendMsgCmdWithBody(uuid string, cmdargs map[string]string, body string) error {
	if len(cmdargs) == 0 {
		return errors.New("Need command arguments")
	}
	_, err := fsock.SendCmdWithArgs(fmt.Sprintf("sendmsg %s\n", uuid), cmdargs, body)
	return err
}

// SendMsgCmd command
func (fsock *FSock) SendMsgCmd(uuid string, cmdargs map[string]string) error {
	return fsock.SendMsgCmdWithBody(uuid, cmdargs, "")
}

// SendEventWithBody command
func (fsock *FSock) SendEventWithBody(eventSubclass string, eventParams map[string]string, body string) (string, error) {
	// Event-Name is overrided to CUSTOM by FreeSWITCH,
	// so we use Event-Subclass instead
	eventParams["Event-Subclass"] = eventSubclass
	return fsock.SendCmdWithArgs(fmt.Sprintf("sendevent %s\n", eventSubclass), eventParams, body)
}

// SendEvent command
func (fsock *FSock) SendEvent(eventSubclass string, eventParams map[string]string) (string, error) {
	return fsock.SendEventWithBody(eventSubclass, eventParams, "")
}

// ReadEvents reads events from socket, attempt reconnect if disconnected
func (fsock *FSock) ReadEvents() (err error) {
	var opened bool
	for {
		if err, opened = <-fsock.errReadEvents; !opened {
			return nil
		} else if err == io.EOF { // Disconnected, try reconnect
			if err = fsock.ReconnectIfNeeded(); err != nil {
				break
			}
		}
	}
	return err
}

func (fsock *FSock) LocalAddr() net.Addr {
	if !fsock.Connected() {
		return nil
	}
	return fsock.conn.LocalAddr()
}
