package model

import (
	"testing"
	"time"
)

func TestLatencyMilliseconds(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(1500 * time.Millisecond)

	if got := LatencyMilliseconds(start, end); got != 1500 {
		t.Fatalf("LatencyMilliseconds() = %d, want 1500", got)
	}
}

func TestLatencyMillisecondsRejectsNegativeDuration(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(-100 * time.Millisecond)

	if got := LatencyMilliseconds(start, end); got != 0 {
		t.Fatalf("LatencyMilliseconds() = %d, want 0", got)
	}
}
