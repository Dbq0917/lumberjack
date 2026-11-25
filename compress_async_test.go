package lumberjack

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// setCompressTestClock replaces the mocked clock under fakeTimeMu.
func setCompressTestClock(ts time.Time) {
	fakeTimeMu.Lock()
	fakeCurrentTime = ts
	fakeTimeMu.Unlock()
}

// waitCompressCh waits for ch or fails the test after a generous timeout.
func waitCompressCh(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitCompressTrue polls fn until it returns true or the timeout elapses.
func waitCompressTrue(t *testing.T, timeout time.Duration, fn func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !fn() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func listCompressDir(t *testing.T, dir string) []os.FileInfo {
	t.Helper()
	entries, err := ioutil.ReadDir(dir)
	isNilUp(err, t, 1)
	return entries
}

func countCompressNames(t *testing.T, dir string) (logs, gz int) {
	for _, info := range listCompressDir(t, dir) {
		name := info.Name()
		if filepath.Ext(name) == ".gz" {
			gz++
		} else if filepath.Ext(name) == ".log" {
			logs++
		}
	}
	return logs, gz
}

func gunzipCompressFile(t *testing.T, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	isNilUp(err, t, 1)
	defer f.Close()
	zr, err := gzip.NewReader(f)
	isNilUp(err, t, 1)
	defer zr.Close()
	b, err := ioutil.ReadAll(zr)
	isNilUp(err, t, 1)
	return b
}

// TestCompressAfterMultipleRotations is the core acceptance check: rotate
// several times in a row with compression enabled, then make sure every old
// backup is compressed exactly once and the file count is correct.
func TestCompressAfterMultipleRotations(t *testing.T) {
	currentTime = fakeTime
	megabyte = 1

	dir := makeTempDir("TestCompressAfterMultipleRotations", t)
	defer os.RemoveAll(dir)

	filename := logFile(dir)
	l := &Logger{
		Compress: true,
		Filename: filename,
		// Large enough (with megabyte mocked to 1 byte) that writes never
		// auto-rotate; this test drives Rotate explicitly.
		MaxSize: 1 << 20,
	}
	defer l.Close()

	const rotations = 5
	payloads := make([][]byte, rotations)
	for i := 0; i < rotations; i++ {
		payloads[i] = []byte(fmt.Sprintf("rotation-payload-%02d", i))
		n, err := l.Write(payloads[i])
		isNil(err, t)
		equals(len(payloads[i]), n, t)

		newFakeTime()
		isNil(l.Rotate(), t)
	}

	// Compression is asynchronous. Wait until the active log is the only
	// plain .log file and all rotations produced .gz backups.
	waitCompressTrue(t, 10*time.Second, func() bool {
		logs, gz := countCompressNames(t, dir)
		return logs == 1 && gz == rotations
	}, "all rotated backups to compress")

	// Close drains queued mill work deterministically.
	isNil(l.Close(), t)

	logs, gz := countCompressNames(t, dir)
	equals(1, logs, t)
	equals(rotations, gz, t)
	// One active log plus rotations compressed backups.
	fileCount(dir, rotations+1, t)

	seen := make(map[string]bool)
	for _, info := range listCompressDir(t, dir) {
		if filepath.Ext(info.Name()) != ".gz" {
			continue
		}
		body := gunzipCompressFile(t, filepath.Join(dir, info.Name()))
		seen[string(body)] = true
	}
	for _, payload := range payloads {
		if !seen[string(payload)] {
			t.Fatalf("compressed backup for %q not found; got %v", payload, seen)
		}
	}
}

// TestMillProtectsInFlightCompression proves the cleanup policy treats a
// backup that is currently being compressed as still present, even when a
// newer rotation pushes it past MaxBackups. A concurrent mill pass must not
// delete it; once compression finishes, the original is replaced by .gz.
func TestMillProtectsInFlightCompression(t *testing.T) {
	currentTime = fakeTime

	base := time.Date(2026, time.November, 26, 12, 0, 0, 0, time.UTC)
	dir := makeTempDir("TestMillProtectsInFlightCompression", t)
	defer os.RemoveAll(dir)

	setCompressTestClock(base.Add(-48 * time.Hour))
	oldBackup := backupFile(dir)
	oldData := []byte("old-backup-payload")
	isNil(ioutil.WriteFile(oldBackup, oldData, 0644), t)

	setCompressTestClock(base.Add(-24 * time.Hour))
	newerBackup := backupFile(dir)
	newerData := []byte("newer-backup-payload")

	startedOld := make(chan struct{})
	releaseOld := make(chan struct{})
	var startOnce, releaseOnce sync.Once

	l := &Logger{
		Filename:   logFile(dir),
		MaxBackups: 1,
		Compress:   true,
		compress: func(src, dst string) error {
			if filepath.Base(src) == filepath.Base(oldBackup) {
				startOnce.Do(func() { close(startedOld) })
				<-releaseOld
			}
			return compressLogFile(src, dst)
		},
	}

	release := func() { releaseOnce.Do(func() { close(releaseOld) }) }
	defer release()

	// First pass claims the only backup and then blocks "compressing" it.
	doneFirst := make(chan error, 1)
	go func() { doneFirst <- l.millRunOnce() }()
	waitCompressCh(t, startedOld, "old backup compression to start")

	oldBase := backupBaseName(filepath.Base(oldBackup))
	equals(true, l.isCompressing(oldBase), t)

	// A newer rotation lands while the old backup is mid-compression.
	isNil(ioutil.WriteFile(newerBackup, newerData, 0644), t)

	// A concurrent retention pass would normally evict the oldest backup...
	isNil(l.millRunOnce(), t)

	// ...but the in-flight marker must spare the old file. The newer backup
	// is compressed by this pass.
	exists(oldBackup, t)
	notExist(newerBackup, t)
	exists(newerBackup+compressSuffix, t)
	equals(newerData, gunzipCompressFile(t, newerBackup+compressSuffix), t)

	// Let the old compression finish; its original is then swapped for .gz.
	release()
	select {
	case err := <-doneFirst:
		isNil(err, t)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first mill pass to finish")
	}

	notExist(oldBackup, t)
	exists(oldBackup+compressSuffix, t)
	equals(oldData, gunzipCompressFile(t, oldBackup+compressSuffix), t)
	equals(false, l.isCompressing(oldBase), t)
}

// TestMillProtectsInFlightFromMaxAge is the "must not be deleted as expired"
// guarantee: a MaxAge sweep spares both the source and the partial .gz output
// of a backup that is marked as compressing, while an unprotected equally-old
// backup is removed. Once the marker clears, the next sweep reclaims it.
func TestMillProtectsInFlightFromMaxAge(t *testing.T) {
	currentTime = fakeTime

	base := time.Date(2026, time.November, 26, 12, 0, 0, 0, time.UTC)
	dir := makeTempDir("TestMillProtectsInFlightFromMaxAge", t)
	defer os.RemoveAll(dir)

	setCompressTestClock(base.Add(-72 * time.Hour))
	victim := backupFile(dir)
	isNil(ioutil.WriteFile(victim, []byte("victim"), 0644), t)

	setCompressTestClock(base.Add(-48 * time.Hour))
	protected := backupFile(dir)
	protectedData := []byte("protected-source")
	isNil(ioutil.WriteFile(protected, protectedData, 0644), t)
	isNil(ioutil.WriteFile(protected+compressSuffix, []byte("partial-gz"), 0644), t)

	setCompressTestClock(base)

	l := &Logger{Filename: logFile(dir), MaxAge: 1}
	protectedBase := backupBaseName(filepath.Base(protected))
	equals(true, l.tryBeginCompress(protectedBase), t)

	isNil(l.millRunOnce(), t)

	// The expired unprotected backup is gone, but the in-flight source and
	// its partial compressed output are both retained.
	notExist(victim, t)
	existsWithContent(protected, protectedData, t)
	exists(protected+compressSuffix, t)

	// Once compression is no longer in flight, the next sweep reclaims it.
	l.endCompress(protectedBase)
	isNil(l.millRunOnce(), t)
	notExist(protected, t)
	notExist(protected+compressSuffix, t)
}

// TestMillCompressionFailureKeepsOriginal checks the orchestration-level
// guarantee: a failing compressor surfaces an error, clears the in-flight
// marker, and never causes mill to remove the source itself.
func TestMillCompressionFailureKeepsOriginal(t *testing.T) {
	currentTime = fakeTime

	base := time.Date(2026, time.November, 26, 12, 0, 0, 0, time.UTC)
	dir := makeTempDir("TestMillCompressionFailureKeepsOriginal", t)
	defer os.RemoveAll(dir)

	setCompressTestClock(base.Add(-time.Hour))
	backup := backupFile(dir)
	data := []byte("must-survive")
	isNil(ioutil.WriteFile(backup, data, 0644), t)

	bomb := fmt.Errorf("injected compression failure")
	l := &Logger{
		Filename: logFile(dir),
		Compress: true,
		compress: func(src, dst string) error { return bomb },
	}

	err := l.millRunOnce()
	notNil(err, t)
	existsWithContent(backup, data, t)
	equals(false, l.isCompressing(backupBaseName(filepath.Base(backup))), t)
}

// TestCompressLogFileFailureLeavesSource exercises the real compressor's
// failure path: when the output cannot be written, compression fails but the
// original log file remains intact.
func TestCompressLogFileFailureLeavesSource(t *testing.T) {
	dir := makeTempDir("TestCompressLogFileFailureLeavesSource", t)
	defer os.RemoveAll(dir)

	src := filepath.Join(dir, "source.log")
	data := []byte("original-log-bytes")
	isNil(ioutil.WriteFile(src, data, 0644), t)

	dst := src + compressSuffix
	// An existing directory at the destination makes creating the output
	// file fail deterministically on every platform.
	isNil(os.Mkdir(dst, 0755), t)

	err := compressLogFile(src, dst)
	notNil(err, t)
	existsWithContent(src, data, t)
}

// TestRotateReturnsWhileCompressionBlocked proves rotation does not
// synchronously wait for compression: with a compressor stalled on a channel,
// Rotate still completes promptly. Close then drains the background work.
func TestRotateReturnsWhileCompressionBlocked(t *testing.T) {
	currentTime = fakeTime
	megabyte = 1

	dir := makeTempDir("TestRotateReturnsWhileCompressionBlocked", t)
	defer os.RemoveAll(dir)

	filename := logFile(dir)
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce, releaseOnce sync.Once

	l := &Logger{
		Filename: filename,
		Compress: true,
		compress: func(src, dst string) error {
			startOnce.Do(func() { close(started) })
			<-release
			// A released, already-closed channel unblocks immediately, so a
			// possible coalesced second pass still finishes.
			return compressLogFile(src, dst)
		},
	}
	defer func() {
		releaseOnce.Do(func() { close(release) })
		_ = l.Close()
	}()

	n, err := l.Write([]byte("boo!"))
	isNil(err, t)
	equals(4, n, t)

	newFakeTime()
	isNil(l.Rotate(), t)
	waitCompressCh(t, started, "compression to block")

	// Advance time so the second rotation gets a distinct backup name.
	newFakeTime()

	rotated := make(chan error, 1)
	go func() { rotated <- l.Rotate() }()

	// release is still closed (not signalled) at this point. If Rotate
	// synchronously waited for compression, this receive would time out.
	select {
	case err := <-rotated:
		isNil(err, t)
	case <-time.After(5 * time.Second):
		t.Fatal("Rotate blocked waiting for background compression")
	}

	releaseOnce.Do(func() { close(release) })
	isNil(l.Close(), t)

	// Both rotations are compressed after Close drains the mill.
	logs, gz := countCompressNames(t, dir)
	equals(1, logs, t)
	equals(2, gz, t)
	fileCount(dir, 3, t)
}
