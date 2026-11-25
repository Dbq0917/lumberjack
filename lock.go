package lumberjack

import "os"

type directoryLock struct {
	file *os.File
}

func acquireDirectoryLock(dir string) (*directoryLock, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	if err := lockExclusive(file); err != nil {
		file.Close()
		return nil, err
	}

	return &directoryLock{file: file}, nil
}

func (l *directoryLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}

	unlockErr := unlockFile(l.file)
	closeErr := l.file.Close()
	l.file = nil
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}

func withDirectoryLock(dir string, fn func() error) error {
	lock, err := acquireDirectoryLock(dir)
	if err != nil {
		return err
	}
	defer lock.Release()

	return fn()
}
