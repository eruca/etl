package etl

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// 1. Mocks (模拟组件)
// =============================================================================

// MockExtractor: 发射指定的整数切片
type MockExtractor struct {
	Data []int
}

func (e *MockExtractor) Extract(ctx context.Context, emit Emitter[int]) error {
	for _, v := range e.Data {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			emit.Emit(v)
		}
	}
	return nil
}

// MockTransformer: 将输入乘以 Multiplier
type MockTransformer struct {
	Multiplier int
}

func (t *MockTransformer) Transform(ctx context.Context, src int) (int, error) {
	if src == -1 {
		return 0, errors.New("simulated transform error")
	}
	// 模拟一点耗时
	time.Sleep(time.Millisecond)
	return src * t.Multiplier, nil
}

// MockLoader: 收集结果到切片中 (并发安全)
type MockLoader struct {
	mu      sync.Mutex
	Results []int
}

func (l *MockLoader) Load(ctx context.Context, item int) error {
	if item == -999 {
		return errors.New("simulated load error")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Results = append(l.Results, item)
	return nil
}

// =============================================================================
// 2. Test Cases (测试用例)
// =============================================================================

// 测试：基础流程 (Extract -> Transform -> Load)
func TestPipeline_BasicFlow(t *testing.T) {
	loader := &MockLoader{}
	transformer := &MockTransformer{Multiplier: 2}

	// S=int, T=int
	p := New[int, int](
		transformer,
		loader,
		WithExtractConcurrency[int](1),
		WithTransformConcurrency[int](1),
		WithLoadConcurrency[int](1),
	)

	// 后台运行 Pipeline
	errCh := make(chan error)
	go func() {
		errCh <- p.Run(context.Background())
	}()

	// 动态提交任务
	input := []int{1, 2, 3, 4, 5}
	p.Submit(&MockExtractor{Data: input}, 3, time.Millisecond*50)

	// 稍微等待处理，然后关闭
	time.Sleep(100 * time.Millisecond)
	p.Shutdown()

	// 等待 Run 返回
	if err := <-errCh; err != nil {
		t.Fatalf("Pipeline run failed: %v", err)
	}

	// 验证结果
	loader.mu.Lock()
	defer loader.mu.Unlock()

	// 期待结果: [2, 4, 6, 8, 10] (顺序可能因为并发而不固定，所以先排序)
	sort.Ints(loader.Results)
	expected := []int{2, 4, 6, 8, 10}

	if len(loader.Results) != len(expected) {
		t.Errorf("Expected length %d, got %d", len(expected), len(loader.Results))
	}
	for i, v := range expected {
		if loader.Results[i] != v {
			t.Errorf("Index %d: expected %d, got %d", i, v, loader.Results[i])
		}
	}
}

// 测试：拦截器 (过滤偶数)
func TestPipeline_Interceptor(t *testing.T) {
	loader := &MockLoader{}
	transformer := &MockTransformer{Multiplier: 1}

	// 定义拦截器：只允许奇数通过
	filterOdd := func(next Emitter[int]) Emitter[int] {
		return EmitterFunc[int](func(item int) {
			if item%2 != 0 {
				next.Emit(item)
			}
		})
	}

	p := New[int, int](
		transformer,
		loader,
		WithEmitterInterceptor(filterOdd), // 注入拦截器
	)

	go func() { p.Run(context.Background()) }()

	// 提交 1-10
	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	p.Submit(&MockExtractor{Data: input}, 3, time.Millisecond*50)

	time.Sleep(50 * time.Millisecond)
	p.Shutdown()

	loader.mu.Lock()
	defer loader.mu.Unlock()

	// 期待结果: 1, 3, 5, 7, 9
	if len(loader.Results) != 5 {
		t.Errorf("Expected 5 odd numbers, got %d", len(loader.Results))
	}
}

// 测试：高并发下的动态提交与数据完整性
func TestPipeline_ConcurrencyAndStress(t *testing.T) {
	var receivedCount int32
	targetCount := 1000

	// 一个简单的 Loader，只计数
	loaderFunc := &SimpleLoader{Fn: func(item int) error {
		atomic.AddInt32(&receivedCount, 1)
		return nil
	}}

	p := New[int, int](
		&MockTransformer{Multiplier: 1},
		loaderFunc,
		WithExtractConcurrency[int](5),    // 5个提取器并发
		WithTransformConcurrency[int](10), // 10个转换器并发
		WithLoadConcurrency[int](5),       // 5个加载器并发
		WithBufferSize[int](100),
	)

	errCh := make(chan error)
	go func() { errCh <- p.Run(context.Background()) }()

	// 模拟并发提交任务
	var wgSub sync.WaitGroup
	for i := 0; i < 10; i++ { // 10个协程提交任务
		wgSub.Add(1)
		go func() {
			defer wgSub.Done()
			// 每个任务包含 100 个数据
			data := make([]int, 100)
			p.Submit(&MockExtractor{Data: data}, 3, time.Millisecond*50)
		}()
	}
	wgSub.Wait()

	// 必须等待所有数据流转完毕
	// 实际生产中最好不要用 sleep，而是等待某种业务完成信号，这里简化测试
	time.Sleep(500 * time.Millisecond)
	p.Shutdown()

	err := <-errCh
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if atomic.LoadInt32(&receivedCount) != int32(targetCount) {
		t.Errorf("Concurrency test failed. Expected %d items, got %d", targetCount, receivedCount)
	}
}

// 测试：错误传播 (Transformer 报错应导致 Run 退出)
func TestPipeline_ErrorHandling(t *testing.T) {
	p := New[int, int](
		&MockTransformer{Multiplier: 1}, // -1 会触发错误
		&MockLoader{},
	)

	errCh := make(chan error)
	go func() { errCh <- p.Run(context.Background()) }()

	// 提交正常数据
	p.Submit(&MockExtractor{Data: []int{1, 2}}, 3, time.Millisecond*50)
	// 提交触发错误的数据 (-1)
	p.Submit(&MockExtractor{Data: []int{-1}}, 3, time.Millisecond*50)

	// 这里不需要调用 Shutdown，因为错误发生时 Run 会自动退出
	err := <-errCh

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if err.Error() != "transform failed: simulated transform error" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// =============================================================================
// 辅助类型
// =============================================================================

type SimpleLoader struct {
	Fn func(int) error
}

func (s *SimpleLoader) Load(ctx context.Context, item int) error {
	return s.Fn(item)
}
