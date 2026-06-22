package processor

import (
	"fmt"
	"testing"

	"github.com/krateoplatformops/plumbing/helm"
)

func TestComputeReleaseDigest_ExcludesTraceparent(t *testing.T) {
	tmpl := "apiVersion: v1\nkind: ConfigMap\n" +
		"metadata:\n  name: x\n  annotations:\n" +
		"    krateo.io/traceparent: %s\n    krateo.io/tracestate: vendor=%s\n" +
		"  labels:\n    krateo.io/composition-id: abc\ndata:\n  k: v\n"

	d1, err := ComputeReleaseDigest(&helm.Release{Manifest: fmt.Sprintf(tmpl, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "a")})
	if err != nil {
		t.Fatalf("digest 1: %v", err)
	}
	d2, err := ComputeReleaseDigest(&helm.Release{Manifest: fmt.Sprintf(tmpl, "00-11111111111111111111111111111111-2222222222222222-01", "b")})
	if err != nil {
		t.Fatalf("digest 2: %v", err)
	}
	if d1 != d2 {
		t.Fatalf("digest must be invariant to the traceparent/tracestate value (else every reconcile churns the helm release): %s vs %s", d1, d2)
	}
}

func TestComputeReleaseDigest_NoTraceparentStable(t *testing.T) {
	plain := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\ndata:\n  k: v\n"
	d1, err := ComputeReleaseDigest(&helm.Release{Manifest: plain})
	if err != nil || d1 == "" {
		t.Fatalf("digest: %v / %q", err, d1)
	}
	d2, _ := ComputeReleaseDigest(&helm.Release{Manifest: plain})
	if d1 != d2 {
		t.Fatalf("plain (no-traceparent) digest must be stable: %s vs %s", d1, d2)
	}
}
