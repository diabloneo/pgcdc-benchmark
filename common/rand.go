package common

import (
	"math/rand"
	"time"
)

// RandSeed seed with current time
func RandSeed() {
	rand.Seed(time.Now().UTC().UnixNano())
}
