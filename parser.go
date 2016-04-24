package relayer

import (
	"fmt"
	"strconv"
)

func readReply(c *Conn) ([]byte, error) {
	line, err := c.ReadLine()
	var bytes []byte

	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, malformed("short response line", string(line))
	}
	switch line[0] {
	case '+', '-', ':':
		return line, nil
	case '$':
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if n < 0 || err != nil {
			return nil, err
		}
		bytes = append(bytes, line...)
		b, err := c.ReadN(n + 2)
		if err != nil {
			return nil, err
		}
		// Now check for trailing CR
		if b[len(b)-2] != '\r' || b[len(b)-1] != '\n' {
			return nil, malformedMissingCRLF()
		}
		bytes = append(bytes, b...)
		return bytes, nil
	case '*':
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if n < 0 || err != nil {
			return nil, err
		}
		bytes = append(bytes, line...)
		for i := 0; i < n; i++ {
			r, err := readReply(c)
			if err != nil {
				return nil, malformed("*<numberOfArguments>", string(line))
			}
			bytes = append(bytes, r...)
		}
		return bytes, nil
	default:
		if len(line) > 0 {
			return line, nil
		}
	}
	return nil, malformed("Empty line", string(line))

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
