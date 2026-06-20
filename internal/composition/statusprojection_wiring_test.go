package composition

import (
	"context"
	"testing"

	"github.com/krateoplatformops/unstructured-runtime/pkg/tools/statusprojection"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// setStatus must project the declarative statusDataTemplate (over self/spec + helm) and
// stamp observedGeneration, alongside the baseline fields.
func TestSetStatus_ProjectsDeclaredFieldsAndObservedGeneration(t *testing.T) {
	h := &handler{
		statusDataTemplate: []statusprojection.Mapping{
			{ForPath: "url", Expression: `${ "https://\(.self.spec.host)" }`},
			{ForPath: "chartVersion", Expression: `${ .helm.version }`},
			{ForPath: "deployed", Expression: `${ .helm.status == "deployed" }`},
			{ForPath: "region", Expression: "eu-west"}, // literal
		},
	}
	mg := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/v1-0-0",
		"kind":       "FireworksApp",
		"metadata":   map[string]any{"name": "demo", "namespace": "apps", "generation": int64(2)},
		"spec":       map[string]any{"host": "demo.example.com"},
	}}

	err := h.setStatus(context.Background(), mg, &statusManagerOpts{
		chartURL:      "oci://reg/fireworksapp",
		chartVersion:  "1.2.0",
		releaseStatus: "deployed",
		digest:        "abc",
		message:       "ok",
		conditionType: ConditionTypeAvailable,
	})
	if err != nil {
		t.Fatalf("setStatus: %v", err)
	}

	get := func(path ...string) any {
		v, found, err := unstructured.NestedFieldNoCopy(mg.Object, append([]string{"status"}, path...)...)
		if err != nil || !found {
			t.Fatalf("status.%v missing (found=%v err=%v)", path, found, err)
		}
		return v
	}

	if got := get("url"); got != "https://demo.example.com" {
		t.Errorf("url = %v", got)
	}
	if got := get("chartVersion"); got != "1.2.0" {
		t.Errorf("chartVersion = %v", got)
	}
	if got := get("deployed"); got != true {
		t.Errorf("deployed = %v, want true", got)
	}
	if got := get("region"); got != "eu-west" {
		t.Errorf("region = %v", got)
	}
	if got := get("observedGeneration"); got != int64(2) {
		t.Errorf("observedGeneration = %v (%T), want int64(2)", got, got)
	}
	// baseline still set
	if got := get("helmChartVersion"); got != "1.2.0" {
		t.Errorf("baseline helmChartVersion = %v", got)
	}
}

// With no declarations, setStatus still stamps observedGeneration and leaves baseline intact.
func TestSetStatus_NoTemplate(t *testing.T) {
	h := &handler{}
	mg := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "d", "namespace": "n", "generation": int64(5)},
	}}
	if err := h.setStatus(context.Background(), mg, &statusManagerOpts{
		chartURL: "u", chartVersion: "v", message: "ok", conditionType: ConditionTypeAvailable,
	}); err != nil {
		t.Fatalf("setStatus: %v", err)
	}
	v, _, _ := unstructured.NestedInt64(mg.Object, "status", "observedGeneration")
	if v != 5 {
		t.Errorf("observedGeneration = %d, want 5", v)
	}
}
