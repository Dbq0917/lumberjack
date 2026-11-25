package lumberjack

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestMaxBackupsLargerThanBackupCount(t *testing.T) {
	currentTime = fakeTime
	megabyte = 1

	dir := makeTempDir("TestMaxBackupsLargerThanBackupCount", t)
	defer os.RemoveAll(dir)

	filename := logFile(dir)
	l := &Logger{
		Filename:   filename,
		MaxSize:    10,
		MaxBackups: 10,
	}
	defer l.Close()

	_, err := l.Write([]byte("boo!"))
	isNil(err, t)

	for i := 0; i < 3; i++ {
		newFakeTime()
		_, err = l.Write([]byte("foooooo!"))
		isNil(err, t)
	}

	// Exercise the same selection synchronously as well so a regression that
	// indexes the slice directly fails deterministically.
	isNil(l.millRunOnce(), t)

	// Cleanup runs in its background goroutine. A cap used directly as a
	// slice index can panic here when there are fewer backups than the cap.
	time.Sleep(20 * time.Millisecond)

	// One active file plus three backups.
	fileCount(dir, 4, t)
}

func TestRetentionRequiresCountAndAge(t *testing.T) {
	oldCurrentTime := currentTime
	oldFakeCurrentTime := fakeCurrentTime
	defer func() {
		currentTime = oldCurrentTime
		fakeCurrentTime = oldFakeCurrentTime
	}()
	currentTime = fakeTime

	base := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

	t.Run("old backup inside count limit is retained", func(t *testing.T) {
		dir := makeTempDir("TestRetentionOldInsideCount", t)
		defer os.RemoveAll(dir)

		backupAtAge := func(ageDays int) string {
			fakeCurrentTime = base.Add(-time.Duration(ageDays) * 24 * time.Hour)
			path := backupFile(dir)
			isNil(os.WriteFile(path, []byte("data"), 0644), t)
			return path
		}

		ancient := backupAtAge(10)
		oldBeyondCount := backupAtAge(8)
		oldInsideCount := backupAtAge(2)
		newest := backupAtAge(0)
		fakeCurrentTime = base

		l := &Logger{
			Filename:   logFile(dir),
			MaxBackups: 2,
			MaxAge:     1,
		}

		isNil(l.millRunOnce(), t)

		notExist(ancient, t)
		notExist(oldBeyondCount, t)
		exists(oldInsideCount, t)
		exists(newest, t)
		fileCount(dir, 2, t)
	})

	t.Run("young backup beyond count limit is retained", func(t *testing.T) {
		dir := makeTempDir("TestRetentionYoungBeyondCount", t)
		defer os.RemoveAll(dir)

		for ageDays := 0; ageDays < 4; ageDays++ {
			fakeCurrentTime = base.Add(-time.Duration(ageDays*2) * 24 * time.Hour)
			isNil(os.WriteFile(backupFile(dir), []byte("data"), 0644), t)
		}
		fakeCurrentTime = base

		l := &Logger{
			Filename:   logFile(dir),
			MaxBackups: 2,
			MaxAge:     10,
		}

		isNil(l.millRunOnce(), t)
		fileCount(dir, 4, t)
	})
}

func TestRemoveFileWithRetryRetriesTransientFailure(t *testing.T) {
	dir := makeTempDir("TestRemoveLogFileRetries", t)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "backup.log")
	isNil(os.WriteFile(path, []byte("data"), 0644), t)

	calls := 0
	remove := func(name string) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("injected transient failure")
		}
		return os.Remove(name)
	}

	isNil(removeFileWithRetry(path, remove, 3, 0), t)
	equals(2, calls, t)
	notExist(path, t)
}

func TestMillRunOnceDeleteFailureCanRunAgain(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("permission-based removal failure test requires a non-root Unix user")
	}

	oldCurrentTime := currentTime
	oldFakeCurrentTime := fakeCurrentTime
	defer func() {
		currentTime = oldCurrentTime
		fakeCurrentTime = oldFakeCurrentTime
	}()
	currentTime = fakeTime

	base := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	fakeCurrentTime = base.Add(-48 * time.Hour)

	dir := makeTempDir("TestMillDeleteFailureRetry", t)
	defer os.RemoveAll(dir)
	backup := backupFile(dir)
	isNil(os.WriteFile(backup, []byte("data"), 0644), t)
	fakeCurrentTime = base

	l := &Logger{
		Filename: logFile(dir),
		MaxAge:   1,
	}

	isNil(os.Chmod(dir, 0555), t)
	defer os.Chmod(dir, 0700)

	err := l.millRunOnce()
	notNil(err, t)
	exists(backup, t)

	isNil(os.Chmod(dir, 0700), t)
	isNil(l.millRunOnce(), t)
	notExist(backup, t)
}
