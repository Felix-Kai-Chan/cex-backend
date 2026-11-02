//go:build ignore
// +build ignore

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBaseURL     = "http://localhost:8080/api/v1"
	defaultDuration    = 10 * time.Second
	defaultConcurrency = 300
	defaultBuyers      = 100
	defaultSellers     = 100
	symbol             = "BTC/USDT"
	buyOrderPrice      = int64(40000)
	sellOrderPrice     = int64(50000)
	seedSellAmount     = int64(100)
	seedSellPerSeller  = 1
)

type createOrderResponse struct {
	OrderID     string `json:"order_id"`
	OrderType   string `json:"order_type"`
	TimeInForce string `json:"time_in_force"`
	Status      string `json:"status"`
	Message     string `json:"message"`
	Error       string `json:"error"`
}

type balanceResponse struct {
	Available float64 `json:"available"`
	Frozen    float64 `json:"frozen"`
	Total     float64 `json:"total"`
	Error     string  `json:"error"`
}

type cancelTarget struct {
	OrderID string
	UserID  string
}

type errorStat struct {
	Count   int64
	Samples []string
}

type result struct {
	Latency time.Duration
	Err     string
}

func main() {
	baseURL := envString("BENCH_URL", defaultBaseURL)
	duration := envDuration("BENCH_DURATION", defaultDuration)
	concurrency := envInt("BENCH_CONCURRENCY", defaultConcurrency)
	buyerCount := envInt("BENCH_BUYERS", defaultBuyers)
	sellerCount := envInt("BENCH_SELLERS", defaultSellers)

	prefix := fmt.Sprintf("bench_%d", time.Now().UnixNano())

	fmt.Println("==========================================")
	fmt.Println("   CEX 单机混合高并发压测")
	fmt.Println("==========================================")
	fmt.Printf("API:          %s\n", baseURL)
	fmt.Printf("并发 Goroutine: %d\n", concurrency)
	fmt.Printf("持续时间:      %s\n", duration)
	fmt.Printf("买家/卖家:     %d / %d\n", buyerCount, sellerCount)
	fmt.Printf("用户前缀:      %s\n\n", prefix)

	client := newHTTPClient()

	buyers := make([]string, 0, buyerCount)
	for i := 0; i < buyerCount; i++ {
		buyers = append(buyers, fmt.Sprintf("%s_buyer_%d", prefix, i))
	}
	sellers := make([]string, 0, sellerCount)
	for i := 0; i < sellerCount; i++ {
		sellers = append(sellers, fmt.Sprintf("%s_seller_%d", prefix, i))
	}
	allUsers := append(append([]string{}, buyers...), sellers...)

	fmt.Println("[1/4] 初始化账户与流动性...")
	for _, u := range buyers {
		if err := addBalance(baseURL, client, u, "USDT", 100_000_000); err != nil {
			fatalf("充值买家失败: %v", err)
		}
	}
	for _, u := range sellers {
		if err := addBalance(baseURL, client, u, "BTC", 1000); err != nil {
			fatalf("充值卖家失败: %v", err)
		}
	}
	for _, u := range sellers {
		for i := 0; i < seedSellPerSeller; i++ {
			resp, err := createOrder(baseURL, client, u, "SELL", "LIMIT", sellOrderPrice, seedSellAmount)
			if err != nil {
				fatalf("挂卖单失败: %v", err)
			}
			if resp.Error != "" {
				fatalf("挂卖单失败: %s", resp.Error)
			}
		}
	}
	fmt.Println("✅ 账户与流动性准备完成")

	fmt.Println("[2/4] 记录初始资金总额...")
	initial, err := sumBalances(baseURL, client, allUsers)
	if err != nil {
		fatalf("读取初始余额失败: %v", err)
	}
	fmt.Printf("   USDT 总额: %.2f\n", initial.USDT)
	fmt.Printf("   BTC  总额: %.2f\n\n", initial.BTC)

	fmt.Printf("[3/4] 开始混合压测（%.0fs）...\n", duration.Seconds())
	start := time.Now()
	stats := runLoad(baseURL, client, buyers, duration, concurrency, prefix)
	elapsed := time.Since(start)

	fmt.Println("[4/4] 校验资金一致性...")
	final, err := sumBalances(baseURL, client, allUsers)
	if err != nil {
		fatalf("读取最终余额失败: %v", err)
	}

	printReport(elapsed, stats, initial, final)

	if !almostEqual(initial.USDT, final.USDT) || !almostEqual(initial.BTC, final.BTC) {
		fmt.Println("\n❌ 资金一致性校验失败")
		os.Exit(1)
	}
	fmt.Println("\n✅ 资金一致性校验通过：所有用户 available + frozen 总额不变")
}

type totals struct {
	USDT float64
	BTC  float64
}

type loadStats struct {
	Total      int64
	Success    int64
	Errors     int64
	Expected   int64
	Latencies  []time.Duration
	ErrorStats map[string]*errorStat
	mu         sync.Mutex
}

func runLoad(baseURL string, client *http.Client, buyers []string, duration time.Duration, concurrency int, prefix string) *loadStats {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	stats := &loadStats{
		ErrorStats: make(map[string]*errorStat),
	}
	cancelCh := make(chan cancelTarget, 200000)
	latCh := make(chan []time.Duration, concurrency)

	var opCounter int64
	var wg sync.WaitGroup
	startGate := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-startGate

			local := make([]time.Duration, 0, 1024)
			for {
				select {
				case <-ctx.Done():
					latCh <- local
					return
				default:
				}

				op := atomic.AddInt64(&opCounter, 1) % 10
				var r result
				switch {
				case op < 5: // 50% LIMIT 买单
					r = doBuy(baseURL, client, buyers, cancelCh, worker)
				case op < 7: // 20% 撤单
					r = doCancel(baseURL, client, cancelCh)
				default: // 30% 查深度
					r = doDepth(baseURL, client)
				}

				atomic.AddInt64(&stats.Total, 1)
				if r.Err != "" {
					if isExpectedError(r.Err) {
						atomic.AddInt64(&stats.Expected, 1)
					} else {
						atomic.AddInt64(&stats.Errors, 1)
						recordError(stats, r.Err)
					}
				} else {
					atomic.AddInt64(&stats.Success, 1)
				}
				local = append(local, r.Latency)
			}
		}(i)
	}

	close(startGate)
	wg.Wait()
	close(latCh)

	for local := range latCh {
		stats.Latencies = append(stats.Latencies, local...)
	}
	return stats
}

func doBuy(baseURL string, client *http.Client, buyers []string, cancelCh chan cancelTarget, worker int) result {
	user := buyers[worker%len(buyers)]
	start := time.Now()
	resp, err := createOrder(baseURL, client, user, "BUY", "LIMIT", buyOrderPrice, 1)
	latency := time.Since(start)
	if err != nil {
		return result{Latency: latency, Err: err.Error()}
	}
	if resp.Error != "" {
		return result{Latency: latency, Err: resp.Error}
	}
	if resp.OrderID != "" && (resp.Status == "NEW" || resp.Status == "PARTIALLY_FILLED") {
		select {
		case cancelCh <- cancelTarget{OrderID: resp.OrderID, UserID: user}:
		default:
		}
	}
	return result{Latency: latency}
}

func doCancel(baseURL string, client *http.Client, cancelCh chan cancelTarget) result {
	var target cancelTarget
	select {
	case target = <-cancelCh:
	default:
		// 没有可撤订单时改为查深度，避免空转
		return doDepth(baseURL, client)
	}

	start := time.Now()
	err := cancelOrder(baseURL, client, target.OrderID, target.UserID)
	latency := time.Since(start)
	if err != nil {
		return result{Latency: latency, Err: err.Error()}
	}
	return result{Latency: latency}
}

func doDepth(baseURL string, client *http.Client) result {
	start := time.Now()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/depth?symbol=BTC%2FUSDT&limit=20", nil)
	if err != nil {
		return result{Latency: time.Since(start), Err: err.Error()}
	}
	resp, err := client.Do(req)
	latency := time.Since(start)
	if err != nil {
		return result{Latency: latency, Err: err.Error()}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 300 {
		return result{Latency: latency, Err: fmt.Sprintf("depth HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))}
	}
	return result{Latency: latency}
}

func createOrder(baseURL string, client *http.Client, user, side, orderType string, price, amount int64) (*createOrderResponse, error) {
	payload := map[string]interface{}{
		"user_id":    user,
		"symbol":     symbol,
		"side":       side,
		"order_type": orderType,
		"price":      price,
		"amount":     amount,
	}
	data, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/orders", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))

	var out createOrderResponse
	_ = json.Unmarshal(body, &out)
	if resp.StatusCode >= 300 {
		if out.Error == "" {
			out.Error = strings.TrimSpace(string(body))
		}
		return &out, nil
	}
	return &out, nil
}

func cancelOrder(baseURL string, client *http.Client, orderID, userID string) error {
	req, err := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/orders/%s?user_id=%s", baseURL, orderID, userID), nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 300 {
		var out struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &out)
		if out.Error != "" {
			return fmt.Errorf("%s", out.Error)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func addBalance(baseURL string, client *http.Client, user, asset string, amount float64) error {
	payload := map[string]interface{}{
		"user_id": user,
		"asset":   asset,
		"amount":  amount,
	}
	data, _ := json.Marshal(payload)
	resp, err := client.Post(baseURL+"/balance/add", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func sumBalances(baseURL string, client *http.Client, users []string) (totals, error) {
	var t totals
	for _, u := range users {
		for _, asset := range []string{"USDT", "BTC"} {
			b, err := getBalance(baseURL, client, u, asset)
			if err != nil {
				return t, err
			}
			sum := b.Available + b.Frozen
			if asset == "USDT" {
				t.USDT += sum
			} else {
				t.BTC += sum
			}
		}
	}
	return t, nil
}

func getBalance(baseURL string, client *http.Client, user, asset string) (balanceResponse, error) {
	var out balanceResponse
	resp, err := client.Get(fmt.Sprintf("%s/balance/%s/%s", baseURL, user, asset))
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 300 {
		return out, fmt.Errorf("balance HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	_ = json.Unmarshal(body, &out)
	if out.Error != "" {
		return out, fmt.Errorf("%s", out.Error)
	}
	return out, nil
}

func recordError(stats *loadStats, msg string) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	key := classifyError(msg)
	es := stats.ErrorStats[key]
	if es == nil {
		es = &errorStat{}
		stats.ErrorStats[key] = es
	}
	es.Count++
	if len(es.Samples) < 3 {
		es.Samples = append(es.Samples, msg)
	}
}

func classifyError(msg string) string {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "deadlock"):
		return "MySQL 死锁"
	case strings.Contains(lower, "lock wait timeout"):
		return "MySQL 锁等待超时"
	case strings.Contains(lower, "redis"):
		return "Redis 错误"
	case strings.Contains(lower, "concurrent"):
		return "并发冲突"
	case strings.Contains(msg, "冲突"):
		return "并发冲突"
	case strings.Contains(msg, "余额不足"):
		return "余额不足"
	default:
		// 去掉动态 ID/数字，避免错误种类爆炸
		compact := strings.TrimSpace(msg)
		if len(compact) > 120 {
			compact = compact[:120]
		}
		return compact
	}
}

func isExpectedError(msg string) bool {
	return strings.Contains(msg, "订单已成交") ||
		strings.Contains(msg, "订单已撤销") ||
		strings.Contains(msg, "订单不存在")
}

func printReport(elapsed time.Duration, stats *loadStats, initial, final totals) {
	total := stats.Total
	if total == 0 {
		fmt.Println("没有产生请求")
		return
	}

	latencies := stats.Latencies
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var sum time.Duration
	for _, d := range latencies {
		sum += d
	}
	avg := sum / time.Duration(len(latencies))
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)
	qps := float64(total) / elapsed.Seconds()
	errorRate := float64(stats.Errors) / float64(total) * 100
	expectedRate := float64(stats.Expected) / float64(total) * 100

	fmt.Println()
	fmt.Println("==========================================")
	fmt.Println("   压测结果")
	fmt.Println("==========================================")
	fmt.Printf("总请求数:      %d\n", total)
	fmt.Printf("成功:          %d\n", stats.Success)
	fmt.Printf("业务错误:      %d (%.2f%%)\n", stats.Errors, errorRate)
	fmt.Printf("预期取消错误:  %d (%.2f%%)\n", stats.Expected, expectedRate)
	fmt.Printf("耗时:          %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("QPS:           %.2f\n", qps)
	fmt.Printf("平均延迟:      %s\n", avg.Round(time.Microsecond))
	fmt.Printf("P50:           %s\n", p50.Round(time.Microsecond))
	fmt.Printf("P95:           %s\n", p95.Round(time.Microsecond))
	fmt.Printf("P99:           %s\n", p99.Round(time.Microsecond))
	fmt.Println()

	fmt.Printf("初始 USDT: %.2f\n", initial.USDT)
	fmt.Printf("最终 USDT: %.2f\n", final.USDT)
	fmt.Printf("初始 BTC:  %.2f\n", initial.BTC)
	fmt.Printf("最终 BTC:  %.2f\n", final.BTC)
	fmt.Println()

	if len(stats.ErrorStats) > 0 {
		fmt.Println("========== 错误明细 ==========")
		type kv struct {
			Key string
			Val *errorStat
		}
		var list []kv
		for k, v := range stats.ErrorStats {
			list = append(list, kv{k, v})
		}
		sort.Slice(list, func(i, j int) bool { return list[i].Val.Count > list[j].Val.Count })

		for _, item := range list {
			fmt.Printf("[%d] %s\n", item.Val.Count, item.Key)
			for _, s := range item.Val.Samples {
				fmt.Printf("    - %s\n", s)
			}
		}
		fmt.Println("==============================")
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-6
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        2000,
			MaxIdleConnsPerHost: 2000,
			MaxConnsPerHost:     2000,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  true,
		},
	}
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "❌ "+format+"\n", args...)
	os.Exit(1)
}
