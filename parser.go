package relayer

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
)

func parseRequest(conn io.ReadCloser) (*Request, error) {
	var buffer bytes.Buffer

	r := bufio.NewReader(conn)
	// first line of redis request should be:
	// *<number of arguments>CRLF
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	buffer.WriteString(line)
	// note that this line also protects us from negative integers
	var argsCount int

	// Multiline request:
	if line[0] == '*' {
		if _, err := fmt.Sscanf(line, "*%d\r\n", &argsCount); err != nil {
			return nil, malformed("*<numberOfArguments>", line)
		}
		// All next lines are pairs of:
		//$<number of bytes of argument 1> CR LF
		//<argument data> CR LF
		// first argument is a command name, so just convert
		firstArg, err := readArgument(r, &buffer)
		if err != nil {
			return nil, err
		}

		for i := 0; i < argsCount-1; i++ {
			if _, err = readArgument(r, &buffer); err != nil {
				return nil, err
			}
		}
		return &Request{
			Name:  strings.ToLower(string(firstArg)[0 : len(firstArg)-2]),
			Bytes: buffer.Bytes(),
			Body:  conn,
		}, nil
	}

	buffer.WriteString(line)
	// Inline request:
	fields := strings.Split(strings.Trim(line, "\r\n"), " ")

	return &Request{
		Name:  strings.ToLower(string(fields[0])),
		Bytes: buffer.Bytes(),
		Body:  conn,
	}, nil

}

func readArgument(r *bufio.Reader, buffer *bytes.Buffer) ([]byte, error) {

	line, err := r.ReadString('\n')
	if err != nil {
		return nil, malformed("$<argumentLength>", line)
	}
	buffer.WriteString(line)
	var argSize int
	if _, err := fmt.Sscanf(line, "$%d\r\n", &argSize); err != nil {
		return nil, malformed("$<argumentSize>", line)
	}

	// I think int is safe here as the max length of request
	// should be less then max int value?
	data, err := ioutil.ReadAll(io.LimitReader(r, int64(argSize)+2))
	if err != nil {
		return nil, err
	}
	dataLen := len(data)

	if dataLen != argSize+2 {
		return nil, malformedLength(argSize, dataLen)
	}

	// Now check for trailing CR
	if data[dataLen-2] != '\r' || data[dataLen-1] != '\n' {
		return nil, malformedMissingCRLF()
	}

	buffer.Write(data)
	return data, nil
}

func malformed(expected string, got string) error {
	Debugf("Mailformed request:'%s does not match %s\\r\\n'", got, expected)
	return fmt.Errorf("Mailformed request:'%s does not match %s\\r\\n'", got, expected)
}

func malformedLength(expected int, got int) error {
	return fmt.Errorf(
		"Mailformed request: argument length '%d does not match %d\\r\\n'",
		got, expected)
}

func malformedMissingCRLF() error {
	return fmt.Errorf("Mailformed request: line should end with \\r\\n")
}
