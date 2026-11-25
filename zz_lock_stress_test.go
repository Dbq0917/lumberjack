package lumberjack

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestZZProcessLockStress(t *testing.T) {
	if os.Getenv("LJ_STRESS_CHILD") == "1" {
		runZZStressChild(t)
		return
	}

	dir := makeTempDir("ZZProcessLockStress", t)
	defer os.RemoveAll(dir)
	logPath := filepath.Join(dir, "stress.log")
	const processes = 2
	const lines = 1000

	var cmds []*exec.Cmd
	var outputs []strings.Builder
	for p := 0; p < processes; p++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestZZProcessLockStress$")
		cmd.Env = append(os.Environ(),
			"LJ_STRESS_CHILD=1",
			"LJ_STRESS_FILE="+logPath,
			"LJ_STRESS_ID="+strconv.Itoa(p),
			"LJ_STRESS_LINES="+strconv.Itoa(lines),
		)
		var out strings.Builder
		cmd.Stdout = &out
		cmd.Stderr = &out
		cmds = append(cmds, cmd)
		outputs = append(outputs, out)
	}

	defer func() {
		for _, cmd := range cmds {
			if cmd.Process != nil && cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}
	}()

	for _, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(cmds))
	for i, cmd := range cmds {
		wg.Add(1)
		go func(i int, cmd *exec.Cmd) {
			defer wg.Done()
			if err := cmd.Wait(); err != nil {
				errs <- fmt.Errorf("writer %d: %w\n%s", i, err, outputs[i].String())
			}
		}(i, cmd)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	seen := make(map[string]int)
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1024), 1024)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "writer-") {
				seen[line]++
			}
		}
		return scanner.Err()
	})
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(files)
	t.Logf("log files: %d", len(files))
	missing := 0
	duplicates := 0
	for p := 0; p < processes; p++ {
		for i := 0; i < lines; i++ {
			line := fmt.Sprintf("writer-%d-line-%06d", p, i)
			count := seen[line]
			if count == 0 {
				missing++
			} else if count > 1 {
				duplicates += count - 1
			}
		}
	}
	if missing != 0 || duplicates != 0 {
		t.Fatalf("missing=%d duplicates=%d seen=%d files=%v", missing, duplicates, len(seen), files)
	}
}

func runZZStressChild(t *testing.T) {
	megabyte = 1
	path := os.Getenv("LJ_STRESS_FILE")
	id, err := strconv.Atoi(os.Getenv("LJ_STRESS_ID"))
	if err != nil {
		t.Fatal(err)
	}
	lines, err := strconv.Atoi(os.Getenv("LJ_STRESS_LINES"))
	if err != nil {
		t.Fatal(err)
	}

	logger := &Logger{Filename: path, MaxSize: 96, MaxBackups: 0}
	for i := 0; i < lines; i++ {
		line := fmt.Sprintf("writer-%d-line-%06d\n", id, i)
		if _, err := logger.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestZZProcessLockCrash(t *testing.T) {
	if os.Getenv("LJ_CRASH_CHILD") == "1" {
		lock, err := acquireDirectoryLock(os.Getenv("LJ_CRASH_DIR"))
		if err != nil {
			t.Fatal(err)
		}
		_ = lock
		fmt.Println("ready")
		os.Stdout.Sync()
		for {
			time.Sleep(time.Second)
			runtime.KeepAlive(lock)
		}
	}

	dir := makeTempDir("ZZProcessLockCrash", t)
	defer os.RemoveAll(dir)
	cmd := exec.Command(os.Args[0], "-test.run=^TestZZProcessLockCrash$")
	cmd.Env = append(os.Environ(), "LJ_CRASH_CHILD=1", "LJ_CRASH_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "ready" {
		t.Fatalf("child did not become ready: %v", scanner.Err())
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	done := make(chan error, 1)
	oldMegabyte := megabyte
	megabyte = 1
	defer func() { megabyte = oldMegabyte }()
	go func() {
		logger := &Logger{Filename: filepath.Join(dir, "crash.log"), MaxSize: 64}
		_, err := logger.Write([]byte("after crash\n"))
		if err == nil {
			err = logger.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("directory lock was not released after SIGKILL")
	}
}

func TestZZInProcessInterleaving(t *testing.T) {
	oldMegabyte := megabyte
	megabyte = 1
	defer func() { megabyte = oldMegabyte }()
	dir := makeTempDir("ZZInProcessInterleaving", t)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "interleave.log")
	l1 := &Logger{Filename: path, MaxSize: 64}
	l2 := &Logger{Filename: path, MaxSize: 64}

	const lines = 500
	for i := 0; i < lines; i++ {
		if _, err := l1.Write([]byte(fmt.Sprintf("one-%06d\n", i))); err != nil {
			t.Fatal(err)
		}
		if _, err := l2.Write([]byte(fmt.Sprintf("two-%06d\n", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "one-") || strings.HasPrefix(line, "two-") {
				seen[line] = true
			}
		}
		return nil
	})
	for i := 0; i < lines; i++ {
		if !seen[fmt.Sprintf("one-%06d", i)] {
			t.Fatalf("missing one %d", i)
		}
		if !seen[fmt.Sprintf("two-%06d", i)] {
			t.Fatalf("missing two %d", i)
		}
	}
}
