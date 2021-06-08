package common

import (
	"crypto/rand"
)

// RandomBytes read given bytes from system's crypto device.
func RandomBytes(n int) ([]byte, error) {
	out := make([]byte, n)
	_, err := rand.Read(out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
