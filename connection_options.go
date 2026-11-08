package acp

import (
	"context"
	"io"
	"sync"
)

// NotificationQueueOverflowPolicy controls how a full inbound notification queue is handled.
type NotificationQueueOverflowPolicy uint8

const (
	// NotificationQueueOverflowClose closes the connection when its notification queue is full.
	NotificationQueueOverflowClose NotificationQueueOverflowPolicy = iota
	// NotificationQueueOverflowBlock applies backpressure until the notification queue has capacity.
	NotificationQueueOverflowBlock
)

// ConnectionOptions configures bounded inbound notification handling.
type ConnectionOptions struct {
	// WaitForNotificationsBeforeRequests delays every inbound request handler, including
	// file and terminal callbacks, until earlier notifications finish. Later notifications
	// remain independent. Cancellation can still be read unless NotificationQueueOverflowBlock
	// has saturated the receive queue. The default false preserves concurrent request dispatch.
	WaitForNotificationsBeforeRequests bool
	MaxQueuedNotifications             int
	NotificationQueueOverflow          NotificationQueueOverflowPolicy
}

// NotificationQueueStats reports bounded inbound notification queue pressure.
type NotificationQueueStats struct {
	Capacity      int
	Queued        int
	HighWaterMark int
}

// NewConnectionWithOptions creates a JSON-RPC connection with explicit queue behavior.
func NewConnectionWithOptions(
	handler MethodHandler,
	peerInput io.Writer,
	peerOutput io.Reader,
	options ConnectionOptions,
) *Connection {
	maxQueuedNotifications := options.MaxQueuedNotifications
	if maxQueuedNotifications <= 0 {
		maxQueuedNotifications = defaultMaxQueuedNotifications
	}
	overflow := options.NotificationQueueOverflow
	if overflow != NotificationQueueOverflowBlock {
		overflow = NotificationQueueOverflowClose
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	inboundCtx, inboundCancel := context.WithCancelCause(context.Background())
	c := &Connection{
		w:                                  peerInput,
		r:                                  peerOutput,
		handler:                            handler,
		pending:                            make(map[string]*pendingResponse),
		inflight:                           make(map[string]context.CancelCauseFunc),
		cancelRequestSignal:                make(chan struct{}, 1),
		ctx:                                ctx,
		cancel:                             cancel,
		inboundCtx:                         inboundCtx,
		inboundCancel:                      inboundCancel,
		notificationQueue:                  make(chan queuedNotification, maxQueuedNotifications),
		notificationQueueOverflow:          overflow,
		waitForNotificationsBeforeRequests: options.WaitForNotificationsBeforeRequests,
	}
	c.notifyCond = sync.NewCond(&c.notifyMu)
	go func() {
		<-c.ctx.Done()
		c.notifyMu.Lock()
		c.notifyCond.Broadcast()
		c.notifyMu.Unlock()
	}()
	go c.sendCancelRequests()
	go c.receive()
	go c.processNotifications()
	return c
}

func (c *Connection) enqueueNotification(msg *anyMessage) error {
	c.notifyMu.Lock()
	c.lastEnqueuedNotificationSeq++
	seq := c.lastEnqueuedNotificationSeq
	c.notifyMu.Unlock()

	queued := queuedNotification{seq: seq, msg: msg}
	c.recordNotificationQueueDepth(len(c.notificationQueue))
	if c.notificationQueueOverflow == NotificationQueueOverflowBlock {
		c.notificationQueue <- queued
		c.recordNotificationQueueDepth(len(c.notificationQueue))
		return nil
	}

	select {
	case c.notificationQueue <- queued:
		c.recordNotificationQueueDepth(len(c.notificationQueue))
		return nil
	default:
		c.notifyMu.Lock()
		defer c.notifyMu.Unlock()
		if c.lastEnqueuedNotificationSeq != seq {
			panic("notification sequence advanced while receive goroutine was queueing")
		}
		c.lastEnqueuedNotificationSeq--
		if c.completedNotificationSeq > c.lastEnqueuedNotificationSeq {
			panic("completed notification sequence exceeded enqueued notification sequence")
		}
		return errNotificationQueueOverflow
	}
}

// NotificationQueueStats returns a point-in-time notification queue snapshot.
func (c *Connection) NotificationQueueStats() NotificationQueueStats {
	if c == nil {
		return NotificationQueueStats{}
	}
	return NotificationQueueStats{
		Capacity:      cap(c.notificationQueue),
		Queued:        len(c.notificationQueue),
		HighWaterMark: int(c.notificationQueueHighWater.Load()),
	}
}

func (c *Connection) recordNotificationQueueDepth(depth int) {
	for {
		current := c.notificationQueueHighWater.Load()
		if int64(depth) <= current || c.notificationQueueHighWater.CompareAndSwap(current, int64(depth)) {
			return
		}
	}
}
