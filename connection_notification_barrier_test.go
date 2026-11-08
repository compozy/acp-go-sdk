package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newNotificationBarrierTestPair(t *testing.T, client Client, agent Agent) (*ClientSideConnection, *AgentSideConnection) {
	t.Helper()

	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()

	clientConn := NewClientSideConnection(client, c2aW, a2cR)
	agentConn := NewAgentSideConnection(agent, a2cW, c2aR)

	t.Cleanup(func() {
		_ = c2aR.Close()
		_ = c2aW.Close()
		_ = a2cR.Close()
		_ = a2cW.Close()
	})

	return clientConn, agentConn
}

func testLoadSessionRequest(sessionID string) LoadSessionRequest {
	return LoadSessionRequest{
		SessionId:  SessionId(sessionID),
		Cwd:        "/",
		McpServers: []McpServer{},
	}
}

func testSessionUpdate(sessionID SessionId, seq int) SessionNotification {
	return SessionNotification{
		Meta:      map[string]any{"seq": seq},
		SessionId: sessionID,
		Update: SessionUpdate{
			AgentMessageChunk: &SessionUpdateAgentMessageChunk{
				Content: TextBlock(fmt.Sprintf("chunk-%d", seq)),
			},
		},
	}
}

func notificationSequence(t *testing.T, n SessionNotification) int {
	t.Helper()

	value, ok := n.Meta["seq"]
	if !ok {
		t.Fatalf("notification missing seq metadata")
	}

	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	default:
		t.Fatalf("unexpected seq metadata type %T", value)
		return 0
	}
}

func waitForPendingRequests(t *testing.T, c *Connection, want int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.pending)
		c.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}

	c.mu.Lock()
	got := len(c.pending)
	c.mu.Unlock()
	t.Fatalf("pending requests = %d, want %d", got, want)
}

func waitForNotificationBarrierDrain(t *testing.T, c *Connection, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.notifyMu.Lock()
		completed := c.completedNotificationSeq
		enqueued := c.lastEnqueuedNotificationSeq
		c.notifyMu.Unlock()
		if completed >= enqueued {
			return
		}
		time.Sleep(time.Millisecond)
	}

	c.notifyMu.Lock()
	completed := c.completedNotificationSeq
	enqueued := c.lastEnqueuedNotificationSeq
	c.notifyMu.Unlock()
	t.Fatalf("notification barrier did not drain: completed=%d enqueued=%d", completed, enqueued)
}

func TestSendRequest_WaitsForPreResponseNotification(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerFinished := make(chan struct{})
	releaseHandler := make(chan struct{})
	allowResponse := make(chan struct{})

	client := &clientFuncs{
		SessionUpdateFunc: func(context.Context, SessionNotification) error {
			close(handlerStarted)
			<-releaseHandler
			close(handlerFinished)
			return nil
		},
	}

	var agentConn *AgentSideConnection
	agent := agentFuncs{
		LoadSessionFunc: func(ctx context.Context, req LoadSessionRequest) (LoadSessionResponse, error) {
			if err := agentConn.SessionUpdate(ctx, testSessionUpdate(req.SessionId, 1)); err != nil {
				return LoadSessionResponse{}, err
			}
			<-allowResponse
			return LoadSessionResponse{}, nil
		},
	}

	clientConn, createdAgentConn := newNotificationBarrierTestPair(t, client, agent)
	agentConn = createdAgentConn

	resultCh := make(chan error, 1)
	go func() {
		_, err := clientConn.LoadSession(context.Background(), testLoadSessionRequest("pre-response"))
		resultCh <- err
	}()

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for pre-response notification handler to start")
	}

	close(allowResponse)
	waitForPendingRequests(t, clientConn.conn, 0, time.Second)

	select {
	case err := <-resultCh:
		t.Fatalf("LoadSession returned before notification handler finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseHandler)

	select {
	case <-handlerFinished:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for pre-response notification handler to finish")
	}

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("LoadSession returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for LoadSession to return after notification handler finished")
	}
}

func TestSendRequest_DoesNotWaitForPostResponseNotification(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerFinished := make(chan struct{})
	releaseHandler := make(chan struct{})
	notificationErrCh := make(chan error, 1)

	client := &clientFuncs{
		SessionUpdateFunc: func(context.Context, SessionNotification) error {
			close(handlerStarted)
			<-releaseHandler
			close(handlerFinished)
			return nil
		},
	}

	var agentConn *AgentSideConnection
	agent := agentFuncs{
		LoadSessionFunc: func(context.Context, LoadSessionRequest) (LoadSessionResponse, error) {
			go func() {
				time.Sleep(25 * time.Millisecond)
				notificationErrCh <- agentConn.SessionUpdate(context.Background(), testSessionUpdate(SessionId("post-response"), 1))
			}()
			return LoadSessionResponse{}, nil
		},
	}

	clientConn, createdAgentConn := newNotificationBarrierTestPair(t, client, agent)
	agentConn = createdAgentConn

	resultCh := make(chan error, 1)
	go func() {
		_, err := clientConn.LoadSession(context.Background(), testLoadSessionRequest("post-response"))
		resultCh <- err
	}()

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("LoadSession returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("LoadSession blocked on post-response notification")
	}

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for post-response notification handler to start")
	}

	select {
	case err := <-notificationErrCh:
		if err != nil {
			t.Fatalf("post-response notification send failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for post-response notification send to complete")
	}

	select {
	case <-handlerFinished:
		t.Fatalf("post-response notification handler finished before release")
	default:
	}

	close(releaseHandler)

	select {
	case <-handlerFinished:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for post-response notification handler to finish")
	}

	select {
	case err := <-notificationErrCh:
		if err != nil {
			t.Fatalf("post-response notification send failed: %v", err)
		}
	default:
	}
}

func TestSendRequest_ConcurrentRequestsDoNotPanic(t *testing.T) {
	const concurrentRequests = 32
	const notificationsPerRequest = 2

	client := &clientFuncs{
		SessionUpdateFunc: func(context.Context, SessionNotification) error {
			time.Sleep(2 * time.Millisecond)
			return nil
		},
	}

	var agentConn *AgentSideConnection
	agent := agentFuncs{
		LoadSessionFunc: func(ctx context.Context, req LoadSessionRequest) (LoadSessionResponse, error) {
			for i := 0; i < notificationsPerRequest; i++ {
				if err := agentConn.SessionUpdate(ctx, testSessionUpdate(req.SessionId, i)); err != nil {
					return LoadSessionResponse{}, err
				}
				time.Sleep(time.Millisecond)
			}
			return LoadSessionResponse{}, nil
		},
	}

	clientConn, createdAgentConn := newNotificationBarrierTestPair(t, client, agent)
	agentConn = createdAgentConn

	start := make(chan struct{})
	errCh := make(chan error, concurrentRequests)
	var wg sync.WaitGroup

	for i := 0; i < concurrentRequests; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := clientConn.LoadSession(ctx, testLoadSessionRequest(fmt.Sprintf("concurrent-%d", i)))
			errCh <- err
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("LoadSession returned error: %v", err)
		}
	}
}

func TestLoadSession_NotificationReplayOrdering(t *testing.T) {
	const replayUpdates = 5

	var (
		mu   sync.Mutex
		seen []int
	)

	client := &clientFuncs{
		SessionUpdateFunc: func(_ context.Context, n SessionNotification) error {
			mu.Lock()
			seen = append(seen, notificationSequence(t, n))
			mu.Unlock()
			return nil
		},
	}

	var agentConn *AgentSideConnection
	agent := agentFuncs{
		LoadSessionFunc: func(ctx context.Context, req LoadSessionRequest) (LoadSessionResponse, error) {
			for i := 0; i < replayUpdates; i++ {
				if err := agentConn.SessionUpdate(ctx, testSessionUpdate(req.SessionId, i)); err != nil {
					return LoadSessionResponse{}, err
				}
			}
			return LoadSessionResponse{}, nil
		},
	}

	clientConn, createdAgentConn := newNotificationBarrierTestPair(t, client, agent)
	agentConn = createdAgentConn

	if _, err := clientConn.LoadSession(context.Background(), testLoadSessionRequest("replay-order")); err != nil {
		t.Fatalf("LoadSession returned error: %v", err)
	}

	mu.Lock()
	got := append([]int(nil), seen...)
	mu.Unlock()

	if len(got) != replayUpdates {
		t.Fatalf("replayed notifications = %d, want %d", len(got), replayUpdates)
	}
	for i, seq := range got {
		if seq != i {
			t.Fatalf("replayed notifications out of order: got %v", got)
		}
	}
}

func TestShutdownDrainsNotifications_WithBarrier(t *testing.T) {
	handlerStarted := make(chan struct{})
	var handlerCompleted atomic.Bool

	client := &clientFuncs{
		SessionUpdateFunc: func(context.Context, SessionNotification) error {
			close(handlerStarted)
			time.Sleep(50 * time.Millisecond)
			handlerCompleted.Store(true)
			return nil
		},
	}

	clientConn, agentConn := newNotificationBarrierTestPair(t, client, agentFuncs{})

	if err := agentConn.SessionUpdate(context.Background(), testSessionUpdate(SessionId("shutdown"), 1)); err != nil {
		t.Fatalf("SessionUpdate returned error: %v", err)
	}

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for shutdown notification handler to start")
	}

	if writer, ok := agentConn.conn.w.(*io.PipeWriter); ok {
		_ = writer.Close()
	} else {
		t.Fatalf("expected io.PipeWriter, got %T", agentConn.conn.w)
	}

	select {
	case <-clientConn.conn.inboundCtx.Done():
		if !handlerCompleted.Load() {
			t.Fatalf("shutdown canceled inbound context before notification handler completed")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for shutdown to drain notifications")
	}
}

func TestSendRequestAsync_ReturnsBeforeResponseAndPreservesNotificationBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	client := &clientFuncs{SessionUpdateFunc: func(context.Context, SessionNotification) error {
		close(started)
		<-release
		return nil
	}}
	var peer *AgentSideConnection
	agent := agentFuncs{LoadSessionFunc: func(ctx context.Context, req LoadSessionRequest) (LoadSessionResponse, error) {
		if err := peer.SessionUpdate(ctx, testSessionUpdate(req.SessionId, 1)); err != nil {
			return LoadSessionResponse{}, err
		}
		return LoadSessionResponse{}, nil
	}}
	conn, peerConn := newNotificationBarrierTestPair(t, client, agent)
	peer = peerConn
	result, err := SendRequestAsync[LoadSessionResponse](conn.conn, ctx, AgentMethodSessionLoad, testLoadSessionRequest("async-barrier"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitForPendingRequests(t, conn.conn, 0, time.Second)
	select {
	case outcome := <-result:
		t.Fatalf("result passed unfinished notification: %#v", outcome)
	default:
	}
	once.Do(func() { close(release) })
	select {
	case outcome := <-result:
		if outcome.Err != nil {
			t.Fatal(outcome.Err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, open := <-result; open {
		t.Fatal("result channel must close after one response")
	}
}

func TestSendRequestAsync_CancelCleansPendingWithoutReadingResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := make(chan struct{})
	agent := agentFuncs{LoadSessionFunc: func(ctx context.Context, _ LoadSessionRequest) (LoadSessionResponse, error) {
		close(started)
		<-ctx.Done()
		return LoadSessionResponse{}, ctx.Err()
	}}
	conn, _ := newNotificationBarrierTestPair(t, &clientFuncs{}, agent)
	result, err := SendRequestAsync[LoadSessionResponse](conn.conn, ctx, AgentMethodSessionLoad, testLoadSessionRequest("async-cancel"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	waitForPendingRequests(t, conn.conn, 0, time.Second)
	select {
	case outcome := <-result:
		if outcome.Err == nil {
			t.Fatal("canceled request returned success")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled request waiter leaked")
	}
}

func TestConnection_InboundRequestNotificationBarrier(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		ordered := ordered
		t.Run(fmt.Sprintf("Should preserve opt-in ordering=%v", ordered), func(t *testing.T) {
			t.Parallel()
			inR, inW := io.Pipe()
			before := make(chan struct{})
			after := make(chan struct{})
			request := make(chan struct{})
			later := make(chan struct{})
			releaseBefore := make(chan struct{})
			releaseAfter := make(chan struct{})
			releaseRequest := make(chan struct{})
			unblockBefore := sync.OnceFunc(func() { close(releaseBefore) })
			unblockAfter := sync.OnceFunc(func() { close(releaseAfter) })
			unblockRequest := sync.OnceFunc(func() { close(releaseRequest) })
			t.Cleanup(func() { unblockBefore(); unblockAfter(); unblockRequest(); _ = inW.Close(); _ = inR.Close() })
			NewConnectionWithOptions(func(ctx context.Context, method string, params json.RawMessage) (any, *RequestError) {
				switch method {
				case "before":
					close(before)
					<-releaseBefore
				case "after":
					close(after)
					<-releaseAfter
				case "permission":
					close(request)
					<-releaseRequest
				case "later":
					close(later)
				}
				return nil, nil
			}, io.Discard, inR, ConnectionOptions{MaxQueuedNotifications: 1, NotificationQueueOverflow: NotificationQueueOverflowBlock, WaitForNotificationsBeforeRequests: ordered})
			write := func(line string) {
				t.Helper()
				if _, err := io.WriteString(inW, line+"\n"); err != nil {
					t.Fatal(err)
				}
			}
			wait := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(2 * time.Second):
					t.Fatal("dispatch did not progress")
				}
			}
			write(`{"jsonrpc":"2.0","method":"before"}`)
			wait(before)
			write(`{"jsonrpc":"2.0","id":1,"method":"permission"}`)
			if ordered {
				select {
				case <-request:
					t.Fatal("request overtook preceding notification")
				case <-time.After(100 * time.Millisecond):
				}
			} else {
				wait(request)
			}
			write(`{"jsonrpc":"2.0","method":"after"}`)
			unblockBefore()
			wait(after)
			wait(request)
			unblockAfter()
			write(`{"jsonrpc":"2.0","method":"later"}`)
			wait(later)
			unblockRequest()
		})
	}
}
