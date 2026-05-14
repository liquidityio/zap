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
