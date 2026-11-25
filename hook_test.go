package lumberjack

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

// waitForEvent waits up to a second for a rotation event on ch.
func waitForEvent(ch chan RotationEvent, t testing.TB) RotationEvent {
	select {
	case event := <-ch:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rotation hook")
		return RotationEvent{}
	}
}

// assertNoEvent fails the test if a rotation event arrives on ch.
func assertNoEvent(ch chan RotationEvent, t testing.TB) {
	select {
	case event := <-ch:
		t.Fatalf("unexpected rotation event: %+v", event)
	case <-time.After(10 * time.Millisecond):
	}
}

func TestRotationHooksFiredWithEvent(t *testing.T) {
	currentTime = fakeTime
	megabyte = 1

	dir := makeTempDir("TestRotationHooksFiredWithEvent", t)
	defer os.RemoveAll(dir)

	filename := logFile(dir)
	beforeCh := make(chan RotationEvent, 4)
	afterCh := make(chan RotationEvent, 4)
	l := &Logger{
		Filename:     filename,
		MaxSize:      10,
		BeforeRotate: func(event RotationEvent) { beforeCh <- event },
		AfterRotate:  func(event RotationEvent) { afterCh <- event },
	}
	defer l.Close()

	// The first write only creates the file, so no hook should fire.
	if _, err := l.Write([]byte("boo!")); err != nil {
		t.Fatal(err)
	}
	assertNoEvent(beforeCh, t)
	assertNoEvent(afterCh, t)

	newFakeTime()
	backup := backupFile(dir)

	// This write exceeds MaxSize and triggers a rotation.
	if _, err := l.Write([]byte("foooooo!")); err != nil {
		t.Fatal(err)
	}

	before := waitForEvent(beforeCh, t)
	after := waitForEvent(afterCh, t)

	expected := RotationEvent{
		OldName: backup,
		NewName: filename,
		Reason:  ReasonSize,
	}
	equals(expected, before, t)
	equals(expected, after, t)

	// A manual rotation reports the manual reason.
	newFakeTime()
	backup = backupFile(dir)
	if err := l.Rotate(); err != nil {
		t.Fatal(err)
	}

	before = waitForEvent(beforeCh, t)
	after = waitForEvent(afterCh, t)
	equals(ReasonManual, before.Reason, t)
	equals(ReasonManual, after.Reason, t)
	equals(backup, before.OldName, t)
	equals(filename, before.NewName, t)
	equals(backup, after.OldName, t)
	equals(filename, after.NewName, t)
}

func TestRotationHookPanicDoesNotFailRotation(t *testing.T) {
	currentTime = fakeTime
	megabyte = 1

	dir := makeTempDir("TestRotationHookPanicDoesNotFailRotation", t)
	defer os.RemoveAll(dir)

	filename := logFile(dir)
	l := &Logger{
		Filename:     filename,
		MaxSize:      10,
		BeforeRotate: func(RotationEvent) { panic("boom before") },
		AfterRotate:  func(RotationEvent) { panic("boom after") },
	}
	defer l.Close()

	if _, err := l.Write([]byte("boo!")); err != nil {
		t.Fatal(err)
	}
	newFakeTime()

	b2 := []byte("foooooo!")
	if _, err := l.Write(b2); err != nil {
		t.Fatalf("rotation failed even though the hook panicked: %v", err)
	}

	// The rotation itself still happened despite the panicking hooks.
	existsWithContent(filename, b2, t)
	existsWithContent(backupFile(dir), []byte("boo!"), t)
	fileCount(dir, 2, t)

	// Give the panicking goroutines time to be recovered.
	time.Sleep(20 * time.Millisecond)
}

// TestSlowRotationHookDoesNotBlockWrites registers hooks that sleep for a
// second and then fires a rotation followed by a burst of writes. The writes
// must complete immediately and every byte must land on disk.
func TestSlowRotationHookDoesNotBlockWrites(t *testing.T) {
	currentTime = fakeTime
	megabyte = 1

	dir := makeTempDir("TestSlowRotationHookDoesNotBlockWrites", t)
	defer os.RemoveAll(dir)

	filename := logFile(dir)
	entered := make(chan struct{}, 4)
	slow := func(RotationEvent) {
		entered <- struct{}{}
		time.Sleep(time.Second)
	}
	l := &Logger{
		Filename:     filename,
		MaxSize:      100,
		BeforeRotate: slow,
		AfterRotate:  slow,
	}
	defer l.Close()

	if _, err := l.Write([]byte("start!")); err != nil {
		t.Fatal(err)
	}
	newFakeTime()
	initial := []byte("start!")

	// Trigger the rotation in the background; the sleeping hooks must not hold
	// the write lock while it is in progress.
	rotateDone := make(chan struct{})
	go func() {
		defer close(rotateDone)
		if err := l.Rotate(); err != nil {
			t.Errorf("rotate failed: %v", err)
		}
	}()

	// Wait until the pre-rotation hook is actually running and sleeping.
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("rotation hook never ran")
	}

	const writes = 30
	line := []byte("x")
	started := time.Now()
	for i := 0; i < writes; i++ {
		if _, err := l.Write(line); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("writes were blocked by the slow rotation hook for %v", elapsed)
	}

	// The rotation still completes in the background and moves the old file.
	select {
	case <-rotateDone:
	case <-time.After(3 * time.Second):
		t.Fatal("rotation never completed")
	}

	// All writes landed on disk. Depending on scheduling they land in the
	// old file before it is renamed (the backup) or in the new active file,
	// so the bytes are counted across both. The backup always holds the
	// initial write.
	deadline := time.Now().Add(3 * time.Second)
	for {
		active, err := ioutil.ReadFile(filename)
		if err != nil {
			t.Fatalf("read active log file: %v", err)
		}
		backup, err := ioutil.ReadFile(backupFile(dir))
		if err != nil {
			t.Fatalf("read backup log file: %v", err)
		}
		expected := append(append([]byte{}, initial...), bytes.Repeat(line, writes)...)
		if bytes.Equal(append(backup, active...), expected) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %q across rotated files, got backup %q active %q",
				expected, backup, active)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
