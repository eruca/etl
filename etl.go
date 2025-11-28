package etl

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// =================================================================================
// 1. 核心接口定义 (Core Interfaces)
// =================================================================================

// Emitter 用于在 Extractor 中发射数据。
// S (Source) 是原始数据类型 (例如 []string 即一组ID)。
type Emitter[S any] interface {
	Emit(item S)
}

// Extractor 负责生产数据。
// 它可以包含复杂的逻辑 (API调用, DB查询等)。
type Extractor[S any] interface {
	Extract(ctx context.Context, emit Emitter[S]) error
}

// Transformer 负责数据转换 (S -> T)。
// S 是源数据类型，T (Target) 是目标数据类型 (例如 Article 结构体)。
// 这是一个纯函数接口，框架会自动并发调用它。
type Transformer[S, T any] interface {
	Transform(ctx context.Context, src S) (T, error)
}

// Loader 负责数据落地 (T -> DB)。
// 这是一个纯函数接口，框架会自动并发调用它。
type Loader[T any] interface {
	Load(ctx context.Context, item T) error
}

// =================================================================================
// 2. 配置与拦截器 (Options & Interceptors)
// =================================================================================

// EmitterInterceptor 允许你在数据进入 Channel 前进行拦截 (日志、计数、过滤等)。
type EmitterInterceptor[S any] func(next Emitter[S]) Emitter[S]

type options[S any] struct {
	extractConcurrency   int
	transformConcurrency int
	loadConcurrency      int
	bufferSize           int
	taskQueueSize        int
	interceptors         []EmitterInterceptor[S]
}

type Option[S any] func(*options[S])

// WithExtractConcurrency 设置并行执行 Extractor 任务的 Worker 数量
func WithExtractConcurrency[S any](n int) Option[S] {
	return func(o *options[S]) {
		if n > 0 {
			o.extractConcurrency = n
		}
	}
}

// WithTransformConcurrency 设置并行执行 Transform 的 Worker 数量
func WithTransformConcurrency[S any](n int) Option[S] {
	return func(o *options[S]) {
		if n > 0 {
			o.transformConcurrency = n
		}
	}
}

// WithLoadConcurrency 设置并行执行 Load 的 Worker 数量
func WithLoadConcurrency[S any](n int) Option[S] {
	return func(o *options[S]) {
		if n > 0 {
			o.loadConcurrency = n
		}
	}
}

// WithBufferSize 设置各阶段之间 Channel 的缓冲区大小
func WithBufferSize[S any](n int) Option[S] {
	return func(o *options[S]) {
		if n > 0 {
			o.bufferSize = n
		}
	}
}

// WithTaskQueueSize 设置待处理任务队列的大小
func WithTaskQueueSize[S any](n int) Option[S] {
	return func(o *options[S]) {
		if n > 0 {
			o.taskQueueSize = n
		}
	}
}

// WithEmitterInterceptor 设置发射器拦截器
func WithEmitterInterceptor[S any](interceptor EmitterInterceptor[S]) Option[S] {
	return func(o *options[S]) {
		o.interceptors = append(o.interceptors, interceptor)
	}
}

// =================================================================================
// 3. Pipeline 执行器 (Pipeline Implementation)
// =================================================================================

// Pipeline 管理整个 ETL 流程的生命周期和并发
type Pipeline[S, T any] struct {
	transformer Transformer[S, T]
	loader      Loader[T]
	opts        options[S]

	// 任务队列：动态接收 Extractor
	taskCh chan Extractor[S]
}

// New 创建一个新的 ETL Pipeline
func New[S, T any](
	transformer Transformer[S, T],
	loader Loader[T],
	opts ...Option[S],
) *Pipeline[S, T] {
	// 1. 初始化默认配置
	defaultOpts := options[S]{
		extractConcurrency:   1,
		transformConcurrency: 1,
		loadConcurrency:      1,
		bufferSize:           10,
		taskQueueSize:        100,
	}

	// 2. 应用用户配置
	for _, apply := range opts {
		apply(&defaultOpts)
	}

	return &Pipeline[S, T]{
		transformer: transformer,
		loader:      loader,
		opts:        defaultOpts,
		taskCh:      make(chan Extractor[S], defaultOpts.taskQueueSize),
	}
}

// Submit 动态提交任务到 Pipeline。
// 如果 Pipeline 正在运行，任务会被 Worker 拾取。
// 如果队列已满，返回 error。
func (p *Pipeline[S, T]) Submit(e Extractor[S], retry int, delay time.Duration) error {
	if retry < 1 {
		panic("retry必须大于等于1")
	}

	for i := 0; i < retry; i++ {
		select {
		case p.taskCh <- e:
			slog.Info("pipeline submit success")
			return nil
		default:
			slog.Error("pipeline submit failed sleep for a while", "i", i)
			time.Sleep(delay)
		}
	}

	return fmt.Errorf("submit %dtimes failed", retry)
}

// Shutdown 优雅关闭：停止接收新任务。
// 注意：这不会立即停止 Pipeline，而是会等待已提交的任务处理完毕。
func (p *Pipeline[S, T]) Shutdown() {
	close(p.taskCh)
}

// chainInterceptors 将所有拦截器包裹在 base Emitter 上
// Apply order: interceptors[0] -> interceptors[1] -> ... -> base
func (p *Pipeline[S, T]) chainInterceptors(base Emitter[S]) Emitter[S] {
	if len(p.opts.interceptors) == 0 {
		return base
	}

	// current 初始指向最内层的 base (即 stdEmitter)
	current := base

	// 倒序遍历：从最后一个拦截器开始包裹
	// 假设 interceptors = [Log, Auth]
	// 1. current = Auth(base)
	// 2. current = Log(current)  => Log(Auth(base))
	for i := len(p.opts.interceptors) - 1; i >= 0; i-- {
		interceptor := p.opts.interceptors[i]
		if interceptor != nil {
			current = interceptor(current)
		}
	}
	return current
}

// Run 启动服务 (阻塞调用)。
// 直到 Shutdown 被调用且所有任务完成，或发生错误，Context 取消时返回。
func (p *Pipeline[S, T]) Run(ctx context.Context) error {
	slog.Info("Pipeline.Run start")
	// 创建可取消的上下文，任何阶段出错都会触发整体 Cancel
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 初始化数据通道
	sourceCh := make(chan S, p.opts.bufferSize)
	targetCh := make(chan T, p.opts.bufferSize)
	errCh := make(chan error, 1) // 缓冲为1，只接收第一个致命错误

	// ---------------------------------------------------------
	// Stage 1: Extraction Workers (Producers)
	// ---------------------------------------------------------
	var wgExtract sync.WaitGroup
	wgExtract.Add(p.opts.extractConcurrency)

	for i := 0; i < p.opts.extractConcurrency; i++ {
		go func(workerID int) {
			slog.Info("pipeline.Run extractor start to work", "workId", workerID)
			defer wgExtract.Done()

			// Worker 持续消费任务队列
			for ext := range p.taskCh {
				slog.Info("pipeline.Run extractor receive message", "ext", ext)
				if ctx.Err() != nil {
					return
				}

				// 构建 Emitter
				var emitter Emitter[S] = &stdEmitter[S]{ch: sourceCh, ctx: ctx}

				// 2. 修改点：调用链式包裹函数
				// 这会自动把 options 里的 interceptor slice 全部应用上去
				emitter = p.chainInterceptors(emitter)

				// 执行提取逻辑
				if err := ext.Extract(ctx, emitter); err != nil {
					p.trySendError(errCh, fmt.Errorf("extractor worker %d failed: %w", workerID, err))
					cancel() // 熔断
					return
				}
			}
		}(i)
	}

	// 监控 Stage 1 完成：当 Shutdown 被调用且所有 Extract Worker 退出时，关闭 sourceCh
	go func() {
		defer close(sourceCh)
		wgExtract.Wait()
	}()

	// ---------------------------------------------------------
	// Stage 2: Transformation Workers (Processors)
	// ---------------------------------------------------------
	var wgTransform sync.WaitGroup
	wgTransform.Add(p.opts.transformConcurrency)

	for i := 0; i < p.opts.transformConcurrency; i++ {
		go func(workId int) {
			defer wgTransform.Done()

			// 持续消费源数据
			for src := range sourceCh {
				slog.Info("etl.transformer receive msg", "workId", workId)
				if ctx.Err() != nil {
					slog.Error("etl.transformer ctx.Erred", "err", ctx.Err())
					return
				}

				slog.Info("etl.transformer start transform", "workId", workId)
				// 执行转换
				res, err := p.transformer.Transform(ctx, src)
				if err != nil {
					p.trySendError(errCh, fmt.Errorf("transform failed: %w", err))
					cancel()
					slog.Error("etl.transformer transform errored", "workId", workId, "err", err)
					return
				}
				slog.Info("etl.transformer transform end", "workId", workId)

				// 发送结果 (带 Context 检查的发送)
				select {
				case targetCh <- res:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// 监控 Stage 2 完成
	go func() {
		defer close(targetCh)
		wgTransform.Wait()
	}()

	// ---------------------------------------------------------
	// Stage 3: Loading Workers (Consumers)
	// ---------------------------------------------------------
	var wgLoad sync.WaitGroup
	wgLoad.Add(p.opts.loadConcurrency)

	for i := 0; i < p.opts.loadConcurrency; i++ {
		go func(wkId int) {
			slog.Info("etl.load start", "workId", wkId)
			defer wgLoad.Done()

			for target := range targetCh {
				if ctx.Err() != nil {
					return
				}

				// 执行加载
				if err := p.loader.Load(ctx, target); err != nil {
					p.trySendError(errCh, fmt.Errorf("load failed: %w", err))
					cancel()
					return
				}
			}
		}(i)
	}

	// 等待加载完成的信号
	go func() {
		defer close(errCh)
		wgLoad.Wait()
	}()

	// ---------------------------------------------------------
	// Main Wait Loop
	// ---------------------------------------------------------
	slog.Info("etl.Wait For all pipeline finished")
	select {
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		return err // 发生错误
	case <-ctx.Done():
		return ctx.Err() // 外部取消
	}
}

// 辅助方法：尝试发送错误，避免阻塞
func (p *Pipeline[S, T]) trySendError(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
		// 通道已满，说明已经有一个错误被捕获了，忽略后续错误
	}
}

// =================================================================================
// 4. 内部工具 (Internal Helpers)
// =================================================================================

// stdEmitter 是 Emitter 接口的默认实现
type stdEmitter[S any] struct {
	ch  chan<- S
	ctx context.Context
}

func (e *stdEmitter[S]) Emit(item S) {
	select {
	case <-e.ctx.Done():
		// 上下文已取消，停止发送，防止死锁
	case e.ch <- item:
		// 成功发送
	}
}

// EmitterFunc 是一个函数类型
// 它不仅是一个函数，还通过实现 Emit 方法，变成了 Emitter 接口的实现者
type EmitterFunc[S any] func(item S)

// Emit 方法只是简单地调用函数本身
func (f EmitterFunc[S]) Emit(item S) {
	f(item)
}
