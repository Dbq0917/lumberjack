//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd
// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd

package lumberjack

import "os"

func lockExclusive(_ *os.File) error {
	return nil
}

func unlockFile(_ *os.File) error {
	return nil
}
