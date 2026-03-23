package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"tinykv/engine"
	"tinykv/server"
)

type benchmarkCase struct {
	Workload       string  `json:"workload"`
	Concurrency    int     `json:"concurrency"`
	ValueSizeBytes int     `json:"value_size_bytes"`
	DurationMS     int64   `json:"duration_ms"`
	Operations     int64   `json:"operations"`
	ThroughputOps  float64 `json:"throughput_ops"`
	P50MS          float64 `json:"p50_ms"`
	P95MS          float64 `json:"p95_ms"`
	P99MS          float64 `json:"p99_ms"`
}

type benchmarkReport struct {
	GeneratedAt string          `json:"generated_at"`
	Duration    string          `json:"duration"`
	Concurrency []int           `json:"concurrency"`
	ValueSize   int             `json:"value_size_bytes"`
	GoVersion   string          `json:"go_version"`
	OS          string          `json:"os"`
	Arch        string          `json:"arch"`
	CPUs        int             `json:"cpus"`
	Cases       []benchmarkCase `json:"cases"`
}

type workloadSpec struct {
	name string
	run  func(*http.Client, string, []byte, int64) error
}

type workerResult struct {
	ops       int64
	latencies []time.Duration
	err       error
}

func main() {
	durationFlag := flag.Duration("duration", 2*time.Second, "per-case benchmark duration")
	concurrencyFlag := flag.String("concurrency", "1,8,32", "comma-separated concurrency levels")
	valueSizeFlag := flag.Int("value-size", 256, "value size in bytes for PUT workloads")
	outputDirFlag := flag.String("output-dir", filepath.Join("docs", "bench"), "directory for generated benchmark artifacts")
	renderFromFlag := flag.String("render-from", "", "reuse an existing results.json file and only regenerate artifacts")
	flag.Parse()

	if strings.TrimSpace(*renderFromFlag) != "" {
		report, err := readJSON(*renderFromFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read report failed: %v\n", err)
			os.Exit(1)
		}
		if err := os.MkdirAll(*outputDirFlag, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "create output dir failed: %v\n", err)
			os.Exit(1)
		}
		if err := writeArtifacts(*outputDirFlag, report); err != nil {
			fmt.Fprintf(os.Stderr, "write artifacts failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	concurrencyLevels, err := parseConcurrency(*concurrencyFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse concurrency failed: %v\n", err)
		os.Exit(1)
	}
	if *durationFlag <= 0 {
		fmt.Fprintln(os.Stderr, "duration must be > 0")
		os.Exit(1)
	}
	if *valueSizeFlag <= 0 {
		fmt.Fprintln(os.Stderr, "value-size must be > 0")
		os.Exit(1)
	}

	report := benchmarkReport{
		GeneratedAt: time.Now().Format(time.RFC3339),
		Duration:    durationFlag.String(),
		Concurrency: concurrencyLevels,
		ValueSize:   *valueSizeFlag,
		GoVersion:   runtime.Version(),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		CPUs:        runtime.NumCPU(),
	}

	value := bytes.Repeat([]byte("x"), *valueSizeFlag)
	workloads := []workloadSpec{
		{name: "PUT", run: runPut},
		{name: "GET", run: runGet},
		{name: "MIXED_80_20", run: runMixed},
	}

	for _, workload := range workloads {
		for _, concurrency := range concurrencyLevels {
			result, err := runCase(workload, concurrency, *durationFlag, value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "benchmark %s concurrency=%d failed: %v\n", workload.name, concurrency, err)
				os.Exit(1)
			}
			report.Cases = append(report.Cases, result)
			fmt.Printf(
				"%-12s c=%-2d ops=%-7d throughput=%9.0f ops/s p50=%6.2fms p95=%6.2fms p99=%6.2fms\n",
				result.Workload,
				result.Concurrency,
				result.Operations,
				result.ThroughputOps,
				result.P50MS,
				result.P95MS,
				result.P99MS,
			)
		}
	}

	if err := os.MkdirAll(*outputDirFlag, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create output dir failed: %v\n", err)
		os.Exit(1)
	}
	if err := writeArtifacts(*outputDirFlag, report); err != nil {
		fmt.Fprintf(os.Stderr, "write artifacts failed: %v\n", err)
		os.Exit(1)
	}
}

func runCase(spec workloadSpec, concurrency int, duration time.Duration, value []byte) (benchmarkCase, error) {
	dbDir, err := os.MkdirTemp("", "tinykv-bench-*")
	if err != nil {
		return benchmarkCase{}, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(dbDir)

	dbPath := filepath.Join(dbDir, "bench.data")
	opts := engine.DefaultOptions(dbPath)
	opts.CacheCapacity = 4096

	db, err := engine.Open(opts)
	if err != nil {
		return benchmarkCase{}, fmt.Errorf("open bench db: %w", err)
	}
	defer db.Close()

	if err := preloadDB(db, value); err != nil {
		return benchmarkCase{}, err
	}

	httpServer := httptest.NewServer(server.NewHandler(db, server.Options{}))
	defer httpServer.Close()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 5 * time.Second,
	}
	defer client.CloseIdleConnections()

	deadline := time.Now().Add(duration)
	var keyCounter int64

	results := make(chan workerResult, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			local := workerResult{latencies: make([]time.Duration, 0, 1024)}
			for {
				if time.Now().After(deadline) {
					break
				}
				opIndex := atomic.AddInt64(&keyCounter, 1)
				start := time.Now()
				if err := spec.run(client, httpServer.URL, value, opIndex); err != nil {
					local.err = err
					break
				}
				local.ops++
				local.latencies = append(local.latencies, time.Since(start))
			}
			results <- local
		}()
	}

	wg.Wait()
	close(results)

	latencies := make([]time.Duration, 0, 4096)
	var ops int64
	for result := range results {
		if result.err != nil {
			return benchmarkCase{}, result.err
		}
		ops += result.ops
		latencies = append(latencies, result.latencies...)
	}
	if len(latencies) == 0 {
		return benchmarkCase{}, fmt.Errorf("no latency samples captured")
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	throughput := float64(ops) / duration.Seconds()
	return benchmarkCase{
		Workload:       spec.name,
		Concurrency:    concurrency,
		ValueSizeBytes: len(value),
		DurationMS:     duration.Milliseconds(),
		Operations:     ops,
		ThroughputOps:  throughput,
		P50MS:          durationToMS(percentile(latencies, 0.50)),
		P95MS:          durationToMS(percentile(latencies, 0.95)),
		P99MS:          durationToMS(percentile(latencies, 0.99)),
	}, nil
}

func preloadDB(db *engine.DB, value []byte) error {
	for i := 0; i < 20000; i++ {
		key := fmt.Sprintf("bench-preload-%05d", i)
		if err := db.Put(key, value); err != nil {
			return fmt.Errorf("preload %s: %w", key, err)
		}
	}
	return nil
}

func runPut(client *http.Client, baseURL string, value []byte, opIndex int64) error {
	key := fmt.Sprintf("write-%09d", opIndex)
	resp, err := client.Do(newRequest(http.MethodPut, baseURL+"/kv/"+key, value))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected PUT status: %d", resp.StatusCode)
	}
	return nil
}

func runGet(client *http.Client, baseURL string, _ []byte, opIndex int64) error {
	key := fmt.Sprintf("bench-preload-%05d", opIndex%20000)
	resp, err := client.Get(baseURL + "/kv/" + key)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected GET status: %d", resp.StatusCode)
	}
	return nil
}

func runMixed(client *http.Client, baseURL string, value []byte, opIndex int64) error {
	if opIndex%5 == 0 {
		return runPut(client, baseURL, value, opIndex)
	}
	return runGet(client, baseURL, value, opIndex)
}

func newRequest(method, target string, body []byte) *http.Request {
	req, err := http.NewRequest(method, target, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	return req
}

func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	index := int(math.Ceil(p*float64(len(latencies)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(latencies) {
		index = len(latencies) - 1
	}
	return latencies[index]
}

func durationToMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func parseConcurrency(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	levels := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid concurrency %q", part)
		}
		if v <= 0 {
			return nil, fmt.Errorf("concurrency must be > 0")
		}
		levels = append(levels, v)
	}
	if len(levels) == 0 {
		return nil, fmt.Errorf("at least one concurrency level is required")
	}
	return levels, nil
}

func writeArtifacts(outputDir string, report benchmarkReport) error {
	if err := writeJSON(filepath.Join(outputDir, "results.json"), report); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(outputDir, "results.csv"), report.Cases); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outputDir, "throughput.svg"), []byte(renderGroupedBarChart("TinyKV HTTP Throughput", "Ops/sec", report.Cases, func(c benchmarkCase) float64 {
		return c.ThroughputOps
	})), 0o644); err != nil {
		return fmt.Errorf("write throughput chart: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "latency-p95.svg"), []byte(renderGroupedBarChart("TinyKV HTTP p95 Latency", "Milliseconds", report.Cases, func(c benchmarkCase) float64 {
		return c.P95MS
	})), 0o644); err != nil {
		return fmt.Errorf("write latency chart: %w", err)
	}
	return nil
}

func writeJSON(path string, report benchmarkReport) error {
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	return nil
}

func readJSON(path string) (benchmarkReport, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return benchmarkReport{}, fmt.Errorf("read json: %w", err)
	}
	var report benchmarkReport
	if err := json.Unmarshal(payload, &report); err != nil {
		return benchmarkReport{}, fmt.Errorf("unmarshal json: %w", err)
	}
	return report, nil
}

func writeCSV(path string, cases []benchmarkCase) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"workload", "concurrency", "value_size_bytes", "duration_ms", "operations", "throughput_ops", "p50_ms", "p95_ms", "p99_ms"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	for _, c := range cases {
		row := []string{
			c.Workload,
			strconv.Itoa(c.Concurrency),
			strconv.Itoa(c.ValueSizeBytes),
			strconv.FormatInt(c.DurationMS, 10),
			strconv.FormatInt(c.Operations, 10),
			fmt.Sprintf("%.2f", c.ThroughputOps),
			fmt.Sprintf("%.3f", c.P50MS),
			fmt.Sprintf("%.3f", c.P95MS),
			fmt.Sprintf("%.3f", c.P99MS),
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	return nil
}

func renderGroupedBarChart(title, yLabel string, cases []benchmarkCase, valueFn func(benchmarkCase) float64) string {
	workloads := uniqueWorkloads(cases)
	concurrencyLevels := uniqueConcurrency(cases)

	const (
		width       = 1000
		height      = 560
		leftPad     = 80
		rightPad    = 40
		topPad      = 88
		bottomPad   = 90
		groupGap    = 36.0
		barGap      = 10.0
		chartHeight = float64(height - topPad - bottomPad)
		chartWidth  = float64(width - leftPad - rightPad)
		titleY      = 38
		legendY     = 24
		legendStep  = 120.0
	)

	maxValue := 0.0
	for _, c := range cases {
		maxValue = math.Max(maxValue, valueFn(c))
	}
	if maxValue == 0 {
		maxValue = 1
	}
	maxValue *= 1.15

	groupWidth := (chartWidth - groupGap*float64(len(workloads)-1)) / float64(len(workloads))
	barWidth := (groupWidth - barGap*float64(len(concurrencyLevels)-1)) / float64(len(concurrencyLevels))
	colors := []string{"#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#8c564b"}

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">`, width, height, width, height)
	b.WriteString(`<rect width="100%" height="100%" fill="#ffffff"/>`)
	fmt.Fprintf(&b, `<text x="%d" y="%d" font-family="Arial, sans-serif" font-size="24" font-weight="700" fill="#222">%s</text>`, leftPad, titleY, html.EscapeString(title))
	fmt.Fprintf(&b, `<text x="18" y="%d" transform="rotate(-90 18,%d)" font-family="Arial, sans-serif" font-size="14" fill="#555">%s</text>`, height/2, height/2, html.EscapeString(yLabel))

	legendX := float64(width-rightPad) - legendStep*float64(len(concurrencyLevels))
	if legendX < float64(leftPad)+420 {
		legendX = float64(leftPad) + 420
	}
	for i, level := range concurrencyLevels {
		x := legendX + float64(i)*legendStep
		color := colors[i%len(colors)]
		fmt.Fprintf(&b, `<rect x="%.2f" y="%d" width="18" height="18" rx="3" fill="%s"/>`, x, legendY, color)
		fmt.Fprintf(&b, `<text x="%.2f" y="%d" font-family="Arial, sans-serif" font-size="13" fill="#333">c=%d</text>`, x+26, legendY+14, level)
	}

	for i := 0; i <= 5; i++ {
		value := maxValue * float64(i) / 5.0
		y := topPad + chartHeight - (value/maxValue)*chartHeight
		fmt.Fprintf(&b, `<line x1="%d" y1="%.2f" x2="%d" y2="%.2f" stroke="#e5e7eb" stroke-width="1"/>`, leftPad, y, width-rightPad, y)
		fmt.Fprintf(&b, `<text x="%d" y="%.2f" font-family="Arial, sans-serif" font-size="12" text-anchor="end" fill="#666">%.1f</text>`, leftPad-10, y+4, value)
	}
	fmt.Fprintf(&b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#111827" stroke-width="2"/>`, leftPad, topPad+int(chartHeight), width-rightPad, topPad+int(chartHeight))
	fmt.Fprintf(&b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#111827" stroke-width="2"/>`, leftPad, topPad, leftPad, topPad+int(chartHeight))

	for groupIndex, workload := range workloads {
		groupX := float64(leftPad) + float64(groupIndex)*(groupWidth+groupGap)
		for barIndex, concurrency := range concurrencyLevels {
			var value float64
			for _, c := range cases {
				if c.Workload == workload && c.Concurrency == concurrency {
					value = valueFn(c)
					break
				}
			}
			barX := groupX + float64(barIndex)*(barWidth+barGap)
			barHeight := (value / maxValue) * chartHeight
			barY := float64(topPad) + chartHeight - barHeight
			color := colors[barIndex%len(colors)]
			fmt.Fprintf(&b, `<rect x="%.2f" y="%.2f" width="%.2f" height="%.2f" rx="4" fill="%s"/>`, barX, barY, barWidth, barHeight, color)
			fmt.Fprintf(&b, `<text x="%.2f" y="%.2f" font-family="Arial, sans-serif" font-size="11" text-anchor="middle" fill="#374151">%.1f</text>`, barX+barWidth/2, barY-6, value)
		}
		fmt.Fprintf(&b, `<text x="%.2f" y="%d" font-family="Arial, sans-serif" font-size="13" text-anchor="middle" fill="#111827">%s</text>`, groupX+groupWidth/2, height-28, html.EscapeString(workload))
	}

	b.WriteString(`</svg>`)
	return b.String()
}

func uniqueWorkloads(cases []benchmarkCase) []string {
	seen := make(map[string]struct{}, len(cases))
	workloads := make([]string, 0, len(cases))
	for _, c := range cases {
		if _, ok := seen[c.Workload]; ok {
			continue
		}
		seen[c.Workload] = struct{}{}
		workloads = append(workloads, c.Workload)
	}
	return workloads
}

func uniqueConcurrency(cases []benchmarkCase) []int {
	seen := make(map[int]struct{}, len(cases))
	levels := make([]int, 0, len(cases))
	for _, c := range cases {
		if _, ok := seen[c.Concurrency]; ok {
			continue
		}
		seen[c.Concurrency] = struct{}{}
		levels = append(levels, c.Concurrency)
	}
	sort.Ints(levels)
	return levels
}
