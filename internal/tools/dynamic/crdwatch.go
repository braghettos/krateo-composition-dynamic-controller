package dynamic

import (
	"context"
	"time"

	"github.com/krateoplatformops/unstructured-runtime/pkg/logging"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// crdGVR is the GroupVersionResource of CustomResourceDefinition objects.
var crdGVR = schema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// crdInvalidateDebounce coalesces bursts of CRD events (e.g. a Pass-A render that
// registers many component CRDs at once, or the informer's initial list) into a
// single discovery invalidation.
const crdInvalidateDebounce = 2 * time.Second

// WatchCRDsAndInvalidate watches CustomResourceDefinition events and invalidates the
// RESTMapper's cached discovery whenever a CRD is added, updated, or deleted.
//
// The mapper built by NewRESTMapper is a DeferredDiscoveryRESTMapper backed by a
// MemCache discovery client: it caches API groups/resources on first use and is never
// refreshed on its own. So a CRD (or a new CRD VERSION) registered after the cache
// warmed is invisible to RESTMapping/IsNamespaced — and, for the umbrella, to the
// `inst.crdExists` Pass-B gate — until the controller is restarted. This watch makes the
// refresh automatic: on any CRD change it calls mapper.Reset(), which invalidates the
// MemCache and drops the delegate so the next lookup re-discovers from the live cluster.
//
// It is a no-op (returns nil) if the mapper does not implement Reset() — e.g. a static
// mapper in tests. Setup errors are returned; once started it runs until ctx is done.
func WatchCRDsAndInvalidate(ctx context.Context, rc *rest.Config, mapper meta.RESTMapper, log logging.Logger) error {
	resetter, ok := mapper.(interface{ Reset() })
	if !ok {
		return nil
	}

	dc, err := dynamic.NewForConfig(rc)
	if err != nil {
		return err
	}

	// Buffered depth 1: many events collapse into one pending invalidation.
	trigger := make(chan struct{}, 1)
	signal := func() {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}

	factory := dynamicinformer.NewDynamicSharedInformerFactory(dc, 10*time.Minute)
	informer := factory.ForResource(crdGVR).Informer()
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(any) { signal() },
		UpdateFunc: func(oldObj, newObj any) {
			// Skip periodic resync no-ops (same resourceVersion) to avoid needless resets.
			oldAcc, errOld := meta.Accessor(oldObj)
			newAcc, errNew := meta.Accessor(newObj)
			if errOld == nil && errNew == nil && oldAcc.GetResourceVersion() == newAcc.GetResourceVersion() {
				return
			}
			signal()
		},
		DeleteFunc: func(any) { signal() },
	}); err != nil {
		return err
	}

	go informer.Run(ctx.Done())

	// Debounced invalidation loop: on a trigger, wait out a quiet window (draining any
	// further triggers) and then Reset() once.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-trigger:
			}

			timer := time.NewTimer(crdInvalidateDebounce)
		drain:
			for {
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-trigger:
					// keep draining the burst
				case <-timer.C:
					break drain
				}
			}

			resetter.Reset()
			if log != nil {
				log.Debug("Invalidated discovery cache after CustomResourceDefinition change")
			}
		}
	}()

	return nil
}
