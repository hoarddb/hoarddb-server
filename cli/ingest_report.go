package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// formatCount renders a count with thousands separators: 1,240,000.
func formatCount(count int64) string {
	digits := strconv.FormatInt(count, 10)
	negative := strings.HasPrefix(digits, "-")
	digits = strings.TrimPrefix(digits, "-")
	var grouped strings.Builder
	for index, digit := range digits {
		if index > 0 && (len(digits)-index)%3 == 0 {
			grouped.WriteByte(',')
		}
		grouped.WriteRune(digit)
	}
	if negative {
		return "-" + grouped.String()
	}
	return grouped.String()
}

// formatBytes renders a size in decimal units, the way the refusal texts in
// ingest-spec.md write them: 41.2 MB.
func formatBytes(size int64) string {
	switch {
	case size >= 1_000_000_000:
		return fmt.Sprintf("%.1f GB", float64(size)/1e9)
	case size >= 1_000_000:
		return fmt.Sprintf("%.1f MB", float64(size)/1e6)
	case size >= 1_000:
		return fmt.Sprintf("%.1f kB", float64(size)/1e3)
	}
	return fmt.Sprintf("%d bytes", size)
}

// formatDuration renders 67s as 1m07s and 0.24s as 0.2s.
func formatDuration(d time.Duration) string {
	if d < 10*time.Second {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	d = d.Round(time.Second)
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%ds", int(d.Seconds()))
}
