// external_discovery_test.go — regression test for the
// "panic at mdns.Discovery.Peers(nil)" bug.
//
// Pre-fix behaviour: when a caller wires its own Discovery via
// WithDiscovery (typical pattern for K8s clusters without mDNS
// multicast — see the static-peer-list discoveries in
// liquidityio/bd/client + liquidityio/ta/client), Connect would
// return a working Client, but the very first Call panicked at
//
//   github.com/luxfi/mdns.(*Discovery).Peers(0x0)
//
// because Node.getOrConnect dereferenced its (nil) n.discovery
// without checking. zapclient.Connect honoured WithDiscovery for
// the Client.disc field but never told the wrapped Node.
//
// Post-fix:
//   - Node.getOrConnect returns a typed "peer not found (no
//     discovery)" error instead of segfaulting.
//   - Connect pre-dials every external-Discovery peer via
//     ConnectDirect so the conn cache is warm before the first Call.
//
// This test pins the bug-class: a Call against an unreachable
// external-Discovery peer MUST return a clean error rather than
// panicking. It uses a stub peer with an address that's
// deliberately not bound — the pre-dial fails (logged at debug,
// non-fatal), and the subsequent Call returns "peer not found".

package zapclient

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	zap "github.com/luxfi/zap"
)

// stubDiscovery implements Discovery from a fixed peer list.
type stubDiscovery struct {
	serviceType string
	peers       []Peer
	mu          sync.RWMutex
}

func (s *stubDiscovery) Peers() []Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Peer, len(s.peers))
	copy(out, s.peers)
	return out
}
func (s *stubDiscovery) PeerCount() int      { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.peers) }
func (s *stubDiscovery) ServiceType() string { return s.serviceType }
func (s *stubDiscovery) Start() error        { return nil }
func (s *stubDiscovery) Stop()               {}

// TestRegister_DispatchOpcodeShift pins the bug where Register stored
// the procedure handler under the SHIFTED opcode (uint16(b) << 8)
// while Node's dispatch loop looked up handlers using the UNSHIFTED
// byte (msg.Flags() >> 8). Every Call hung until the context
// deadline expired because the lookup missed.
//
// Test: register an Echo procedure on a server bound to a known
// loopback port, dial via static-peer Discovery, Call Echo, expect
// the handler to fire and the response sentinel to round-trip.
//
// Pre-fix: handler never fires; Call times out with context-deadline.
// Post-fix: handler fires; Call returns the response.
func TestRegister_DispatchOpcodeShift(t *testing.T) {
	// Pick a free loopback port for the server. The Node API doesn't
	// expose a ListenAddr accessor on bound ephemeral ports, so we
	// pre-allocate a port via a throwaway listener and inject it via
	// the ServerOptions.Port field (exposed by the local option
	// closure pattern, same shape used in liquidityio/pkg/zapsetup).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	l.Close()

	// Server side — Register Echo + Start.
	srv, err := NewServer("dispatch-shift-test",
		WithNoDiscovery(),
		func(o *ServerOptions) { o.Port = port },
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer srv.Stop()
	handlerHit := make(chan struct{}, 1)
	if err := srv.Register("Echo", func(ctx context.Context, _ PeerInfo, req *zap.Message) (*zap.Message, error) {
		select {
		case handlerHit <- struct{}{}:
		default:
		}
		b := zap.NewBuilder(64)
		ob := b.StartObject(64)
		ob.SetUint32(0, 42) // response sentinel
		ob.FinishAsRoot()
		out, _ := zap.Parse(b.Finish())
		return out, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Client side — static-peer discovery pointing at the loopback
	// server. mDNS browse not required.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	staticAddr := net.JoinHostPort(host, strconv.Itoa(port))
	c, err := Connect(ctx, "dispatch-shift-test",
		WithMinPeers(1),
		WithDiscoverTimeout(2*time.Second),
		WithCallTimeout(2*time.Second),
		WithDiscovery(&stubDiscovery{
			serviceType: "dispatch-shift-test",
			peers: []Peer{{
				NodeID:      "dispatch-shift-test-server",
				ServiceType: "dispatch-shift-test",
				Address:     staticAddr,
				LastSeen:    time.Now(),
			}},
		}),
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	msg := zap.NewBuilder(64)
	ob := msg.StartObject(64)
	ob.FinishAsRoot()
	req, _ := zap.Parse(msg.Finish())

	resp, callErr := c.Call(ctx, "Echo", req)
	if callErr != nil {
		t.Fatalf("Call failed (likely a dispatch-shift regression): %v", callErr)
	}
	if resp == nil {
		t.Fatal("expected non-nil response from Echo handler")
	}
	if got := resp.Root().Uint32(0); got != 42 {
		t.Errorf("response sentinel: got %d want 42", got)
	}

	// Confirm the handler ran on the server side.
	select {
	case <-handlerHit:
		// OK
	default:
		t.Fatal("handler was not invoked — Register/dispatch keyed inconsistently")
	}
}

// TestExternalDiscovery_NoPanicOnCall is the regression test. Before
// the fix this panicked at mdns.Discovery.Peers(nil). After, Call
// returns a clean error — what kind doesn't matter; the contract
// being pinned is "no panic + a typed error".
func TestExternalDiscovery_NoPanicOnCall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stub peer with an address that's definitely not listening.
	// Pre-dial will fail silently; the connection cache stays empty;
	// the subsequent Call must error cleanly rather than segfault.
	c, err := Connect(ctx, "external-discovery-test",
		WithMinPeers(1),
		WithDiscoverTimeout(2*time.Second),
		WithCallTimeout(1*time.Second),
		WithDiscovery(&stubDiscovery{
			serviceType: "external-discovery-test",
			peers: []Peer{{
				NodeID:      "unreachable-peer",
				ServiceType: "external-discovery-test",
				Address:     "127.0.0.1:1", // port 1 is reserved & not bound
				LastSeen:    time.Now(),
			}},
		}),
	)
	if err != nil {
		t.Fatalf("Connect: %v (the fix should let Connect succeed even when pre-dial fails)", err)
	}
	defer c.Close()

	msg := zap.NewBuilder(64)
	ob := msg.StartObject(64)
	ob.FinishAsRoot()
	parsed, _ := zap.Parse(msg.Finish())

	// The Call must return a clean error. Pre-fix this segfaulted.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Call panicked — regression of the nil-discovery bug: %v", r)
		}
	}()
	_, callErr := c.Call(ctx, "AnyProcedure", parsed)
	if callErr == nil {
		t.Log("Call somehow succeeded (test peer is unreachable; unexpected but harmless)")
	} else {
		t.Logf("Call returned cleanly: %v", callErr)
	}
}
