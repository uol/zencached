package zencached

import (
	"bytes"
	"fmt"
	"time"
)

//
// This file has all implemented commands from memcached.
// More information here:
// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
// author: rnojiri
//

// memcached commands and constants
const (
	storageFmt string = "%s %s 0 %d %d\r\n%s\r\n"
	getFmt     string = "get %s\r\n"
	deleteFmt  string = "delete %s\r\n"
	empty      string = ""
)

// memcached responses
var (
	mcrStored    []byte = []byte("STORED")
	mcrNotStored []byte = []byte("NOT_STORED")
	mcrValue     []byte = []byte("VALUE")
	mcrEnd       []byte = []byte("END")
	mcrNotFound  []byte = []byte("NOT_FOUND")
	mcrDeleted   []byte = []byte("DELETED")
	lineBreaks   []byte = []byte{[]byte("\r")[0], []byte("\n")[0]}
)

// memcachedCommand type
type memcachedCommand string

const (
	// Add - add some key if it not exists
	Add memcachedCommand = "add"

	// Set - sets a key if it exists or not
	Set memcachedCommand = "set"

	// get - return a key if it exists or not
	get memcachedCommand = "get"

	// delete - return a key if it exists or not
	delete memcachedCommand = "delete"
)

// countOperation - send the operation count metric
func (z *Zencached) countOperation(host string, operation memcachedCommand) {

	go z.metricsCollector.Count(
		metricOperationCount,
		1,
		tagNodeName, host,
		tagOperationName, string(operation),
	)
}

// executeSend - sends a message to memcached
func (z *Zencached) executeSend(telnetConn *Telnet, operation memcachedCommand, renderedCmd string) error {

	if !z.enableMetrics {

		err := telnetConn.Send(renderedCmd)
		if err != nil {
			return err
		}

	} else {

		start := time.Now()
		err := telnetConn.Send(renderedCmd)
		if err != nil {
			return err
		}
		elapsedTime := time.Since(start)

		go z.metricsCollector.Maximum(
			metricOperationTime,
			float64(elapsedTime.Milliseconds()),
			tagNodeName, telnetConn.GetHost(),
			tagOperationName, string(operation),
		)
	}

	return nil
}

// checkResponse - checks the memcached response
func (z *Zencached) checkResponse(telnetConn *Telnet, foundResponse, notFoundResponse []byte, operation memcachedCommand, renderedCmd string) (bool, []byte, error) {

	response, err := telnetConn.Read()
	if err != nil {
		return false, nil, err
	}

	if !bytes.HasPrefix(response, foundResponse) {
		if !bytes.HasPrefix(response, notFoundResponse) {
			return false, nil, fmt.Errorf("%s operation error on key:\n%s", operation, string(response))
		}

		if z.enableMetrics {
			go z.metricsCollector.Count(
				metricCacheMiss,
				1,
				tagNodeName, telnetConn.GetHost(),
				tagOperationName, string(operation),
			)
		}

		return false, response, nil
	}

	if z.enableMetrics {
		go z.metricsCollector.Count(
			metricCacheHit,
			1,
			tagNodeName, telnetConn.GetHost(),
			tagOperationName, string(operation),
		)
	}

	return true, response, nil
}

// Storage - performs an storage operation
func (z *Zencached) Storage(cmd memcachedCommand, routerHash []byte, key string, value string, ttl uint16) (bool, error) {

	telnetConn, index := z.GetTelnetConnection(routerHash, key)
	defer z.ReturnTelnetConnection(telnetConn, index)

	if z.enableMetrics {
		z.countOperation(telnetConn.GetHost(), cmd)
	}

	renderedCmd := fmt.Sprintf(storageFmt, cmd, key, ttl, len(value), value)
	err := z.executeSend(telnetConn, cmd, renderedCmd)
	if err != nil {
		return false, err
	}

	wasStored, _, err := z.checkResponse(telnetConn, mcrStored, mcrNotStored, cmd, renderedCmd)
	if err != nil {
		return false, err
	}

	return wasStored, nil
}

// Get - performs a get operation
func (z *Zencached) Get(routerHash []byte, key string) (string, bool, error) {

	telnetConn, index := z.GetTelnetConnection(routerHash, key)
	defer z.ReturnTelnetConnection(telnetConn, index)

	if z.enableMetrics {
		z.countOperation(telnetConn.GetHost(), get)
	}

	renderedCmd := fmt.Sprintf(getFmt, key)
	err := z.executeSend(telnetConn, get, renderedCmd)
	if err != nil {
		return empty, false, err
	}

	exists, response, err := z.checkResponse(telnetConn, mcrValue, mcrEnd, get, renderedCmd)
	if !exists || err != nil {
		return empty, false, err
	}

	start, end := z.extractLine(response, 2)
	storedValue := string(response[start:end])

	return storedValue, true, nil
}

// extractLine - extracts a line from the response
func (z *Zencached) extractLine(response []byte, lineNumber int) (start, end int) {

	lineBreakFound := false
	currentLine := 0
	for i := 0; i < len(response); i++ {
		if !lineBreakFound && (response[i] == lineBreaks[0] || response[i] == lineBreaks[1]) {

			end = i

			if response[i+1] == lineBreaks[0] || response[i+1] == lineBreaks[1] {
				i++
			}

			lineBreakFound = true
			currentLine++
			if currentLine == lineNumber {
				return
			}
		} else {
			if lineBreakFound {
				lineBreakFound = false
				start = i
			}
		}
	}

	return
}

// Delete - performs a delete operation
func (z *Zencached) Delete(routerHash []byte, key string) (bool, error) {

	telnetConn, index := z.GetTelnetConnection(routerHash, key)
	defer z.ReturnTelnetConnection(telnetConn, index)

	if z.enableMetrics {
		z.countOperation(telnetConn.GetHost(), delete)
	}

	renderedCmd := fmt.Sprintf(deleteFmt, key)
	err := z.executeSend(telnetConn, delete, renderedCmd)
	if err != nil {
		return false, err
	}

	exists, _, err := z.checkResponse(telnetConn, mcrDeleted, mcrNotFound, delete, renderedCmd)
	if err != nil {
		return false, err
	}

	return exists, nil
}
