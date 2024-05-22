package moqorigins

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSleepWithContextLongerDuration(t *testing.T) {
	ctx := context.Background()

	err := sleepWithContext(ctx, 50*time.Millisecond)
	if err != nil {
		t.Errorf("sleepWithContext returns an error for context that is not cancelled with longer duration: %v", err)
	}
}

func TestSleepWithContextHighLoads(t *testing.T) {
	var wg sync.WaitGroup
	count := 150
	wg.Add(count)

	errorChannel := make(chan error, count)

	for i := 0; i < count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		go func() {
			defer wg.Done()
			defer cancel()
			err := sleepWithContext(ctx, 15*time.Millisecond)
			errorChannel <- err
		}()
	}

	wg.Wait()
	close(errorChannel)

	for err := range errorChannel {
		if err == nil {
			t.Error("expected an error due to context timeout, but got nil")
		}
	}
}

func TestSleepWithContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sleepWithContext(ctx, 150*time.Millisecond)
	if err == nil {
		t.Errorf("sleepWithContext should return an error after context is cancelled")
	} else if err.Error() != "Interrupted" {
		t.Errorf("expected error 'Interrupted', but got: %v", err)
	}
}

func TestSleepWithContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	startTime := time.Now()
	err := sleepWithContext(ctx, 15*time.Millisecond)
	duration := time.Since(startTime)

	if err == nil {
		t.Errorf("sleepWithContext should return an error after context timed out")
	} else if duration < 5*time.Millisecond {
		t.Errorf("function returned too quickly, expecting at least a 5ms delay, got %v", duration)
	}
}
