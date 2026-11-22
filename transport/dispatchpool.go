package transport

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// dispatchIdleLife is how long a worker waits for another frame before it exits. It is
// long enough to bridge the gaps of a busy connection and short enough that an idle server
// holds no workers.
const dispatchIdleLife = 10 * time.Second

// dispatchPool runs frame handlers on goroutines that outlive one frame. A goroutine
// starts on a small stack and grows it by copying, once per level of the call chain, and a
// handler's chain (route, check, topology, store) is deep; a goroutine per frame paid that
// growth on every request. A worker keeps its grown stack from one frame to the next.
//
// The pool never queues and never caps: a frame goes to an idle worker if one is waiting,
// and otherwise to a new worker, so the concurrency the read-ahead window admits is
// unchanged and a handler that blocks cannot starve another. The window and the
// connection's own wait group still bound and drain the work. Workers are counted in
// tracked and exit when ctx ends or after dispatchIdleLife unused. Its cost is a
// handful of goroutines per node and nothing per connection or per bucket.
type dispatchPool struct {
	ctx     context.Context
	tracked *sync.WaitGroup
	tasks   chan func() // unbuffered: a send succeeds only when a worker is waiting
	idle    time.Duration
	spawned atomic.Int64 // workers started so far; a pool that reuses them keeps it small
}

func newDispatchPool(ctx context.Context, tracked *sync.WaitGroup) *dispatchPool {
	return &dispatchPool{ctx: ctx, tracked: tracked, tasks: make(chan func()), idle: dispatchIdleLife}
}

// submit runs task on an idle worker, or on a new one. The caller holds a slot in tracked
// already (its connection's), so the Add below cannot race the Wait in Close.
func (p *dispatchPool) submit(task func()) {
	select {
	case p.tasks <- task:
		return
	default:
	}
	p.tracked.Add(1)
	p.spawned.Add(1)
	go p.work(task)
}

func (p *dispatchPool) work(task func()) {
	defer p.tracked.Done()
	idle := time.NewTimer(p.idle)
	defer idle.Stop()
	for {
		task()
		if !idle.Stop() {
			select {
			case <-idle.C:
			default:
			}
		}
		idle.Reset(p.idle)
		select {
		case task = <-p.tasks:
		case <-idle.C:
			return
		case <-p.ctx.Done():
			return
		}
	}
}
