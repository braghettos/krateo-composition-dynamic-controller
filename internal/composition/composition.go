package composition

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/krateoplatformops/unstructured-runtime/pkg/tools/unstructured/condition"

	xcontext "github.com/krateoplatformops/unstructured-runtime/pkg/context"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/chartinspector"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/metrics"
	compositionMeta "github.com/krateoplatformops/composition-dynamic-controller/pkg/meta"
	unstructuredtools "github.com/krateoplatformops/unstructured-runtime/pkg/tools/unstructured"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/rbacgen"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools/archive"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools/processor"
	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools/tracer"

	"github.com/krateoplatformops/composition-dynamic-controller/internal/tools/rbac"
	"github.com/krateoplatformops/plumbing/env"
	helmconfig "github.com/krateoplatformops/plumbing/helm"
	"github.com/krateoplatformops/plumbing/helm/utils"
	helmutils "github.com/krateoplatformops/plumbing/helm/utils"
	"github.com/krateoplatformops/plumbing/helm/v3"

	"github.com/krateoplatformops/plumbing/kubeutil/event"
	"github.com/krateoplatformops/plumbing/maps"
	"github.com/krateoplatformops/unstructured-runtime/pkg/controller"
	"github.com/krateoplatformops/unstructured-runtime/pkg/meta"
	"github.com/krateoplatformops/unstructured-runtime/pkg/telemetry"
	apimeta "k8s.io/apimachinery/pkg/api/meta"

	"github.com/krateoplatformops/unstructured-runtime/pkg/pluralizer"
	"github.com/krateoplatformops/unstructured-runtime/pkg/tools"
	"github.com/krateoplatformops/unstructured-runtime/pkg/tools/statusprojection"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var (
	krateoNamespace = env.String(krateoNamespaceEnvVar, krateoNamespaceDefault)
	helmMaxHistory  = env.Int(helmMaxHistoryEnvvar, 3)
)

const (
	reasonReconciliationGracefullyPaused event.Reason = "ReconciliationGracefullyPaused"

	// Event reasons
	reasonCreated = "CompositionCreated"
	reasonDeleted = "CompositionDeleted"
	reasonUpdated = "CompositionUpdated"

	// Environment variables
	helmMaxHistoryEnvvar  = "HELM_MAX_HISTORY"
	krateoNamespaceEnvVar = "KRATEO_NAMESPACE"

	// Default namespace for Krateo Installation
	krateoNamespaceDefault = "krateo-system"
)

var _ controller.ExternalClient = (*handler)(nil)

type HandlerOptions struct {
	Kubeconfig        *rest.Config
	PackageInfoGetter archive.Getter
	EventRecorder     event.APIRecorder
	Pluralizer        pluralizer.PluralizerInterface
	ChartInspectorUrl string
	SaName            string
	SaNamespace       string
	SafeReleaseName   bool
	Mapper            apimeta.RESTMapper
	// StatusDataTemplate are the declarative status projections (snowplow
	// widgetDataTemplate shape) shipped by core-provider from the CompositionDefinition.
	StatusDataTemplate []statusprojection.Mapping
	// APIResolver, when set, resolves the CompositionDefinition's apiRef each reconcile to the
	// keyed ".api" projection source. nil disables apiRef resolution (the ".api" source is
	// simply absent and api-dependent mappings degrade individually).
	APIResolver APIResolver
}

// APIResolver resolves the apiRef's RESTAction (via snowplow, under the CDC's authn identity)
// to the keyed `.api.<callName>` source map for a specific composition instance.
type APIResolver interface {
	Resolve(ctx context.Context, mg *unstructured.Unstructured) (map[string]any, error)
}

func NewHandler(opts *HandlerOptions) controller.ExternalClient {
	return &handler{
		kubeconfig:         opts.Kubeconfig,
		pluralizer:         opts.Pluralizer,
		packageInfoGetter:  opts.PackageInfoGetter,
		eventRecorder:      opts.EventRecorder,
		chartInspectorUrl:  opts.ChartInspectorUrl,
		saName:             opts.SaName,
		saNamespace:        opts.SaNamespace,
		safeReleaseName:    opts.SafeReleaseName,
		mapper:             opts.Mapper,
		statusDataTemplate: opts.StatusDataTemplate,
		apiResolver:        opts.APIResolver,
	}
}

type handler struct {
	kubeconfig    *rest.Config
	pluralizer    pluralizer.PluralizerInterface
	eventRecorder event.APIRecorder
	mapper        apimeta.RESTMapper

	packageInfoGetter archive.Getter

	chartInspectorUrl string
	saName            string
	saNamespace       string
	// Feature flag to disable random suffix in Helm release names. This is highly discouraged as it can lead to release name collisions, but it can be useful for certain complex charts that have issues with long release names.
	safeReleaseName bool
	// statusDataTemplate are the declarative ${ jq } status projections from the
	// CompositionDefinition, evaluated each reconcile and written under .status.
	statusDataTemplate []statusprojection.Mapping
	// apiResolver resolves the apiRef to the ".api" projection source each reconcile; nil
	// when no apiRef is declared.
	apiResolver APIResolver
}

func (h *handler) Observe(ctx context.Context, mg *unstructured.Unstructured) (controller.ExternalObservation, error) {
	mg = mg.DeepCopy()

	log := xcontext.Logger(ctx)

	log = log.WithValues("op", "Observe").
		WithValues("apiVersion", mg.GetAPIVersion()).
		WithValues("kind", mg.GetKind()).
		WithValues("name", mg.GetName()).
		WithValues("namespace", mg.GetNamespace())

	// If the Composition is being deleted, do NOT run the drift observation. Observe computes
	// drift via a helm Upgrade, which fails with "has no deployed releases" once the release is
	// mid-uninstall — and resync re-enqueues Observe every cycle, so the reconcile loops in
	// ReconcileError and never finalizes. Report the resource as existing + up-to-date so the
	// runtime routes the deletion to the Delete handler, which performs/completes the uninstall
	// (it tolerates an already-removed release) and clears the finalizer.
	if mg.GetDeletionTimestamp() != nil {
		log.Debug("Composition has a deletionTimestamp; skipping drift observe — deletion is handled by Delete.")
		return controller.ExternalObservation{
			ResourceExists:   true,
			ResourceUpToDate: true,
		}, nil
	}

	dyn, err := dynamic.NewForConfig(h.kubeconfig)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("creating dynamic client: %w", err)
	}

	updateOpts := tools.UpdateOptions{
		Pluralizer:    h.pluralizer,
		DynamicClient: dyn,
	}

	compositionMeta.SetReleaseName(mg, compositionMeta.CalculateReleaseName(mg, h.safeReleaseName))
	releaseName := compositionMeta.GetReleaseName(mg)
	if _, p := compositionMeta.GetGracefullyPausedTime(mg); p && compositionMeta.IsGracefullyPaused(mg) {
		log.Debug("Composition is gracefully paused, skipping observe.")
		h.eventRecorder.Event(mg, event.Normal(reasonReconciliationGracefullyPaused, "Observe", "Reconciliation is paused via the gracefully paused annotation."))
		return controller.ExternalObservation{
			ResourceExists:   true,
			ResourceUpToDate: true,
		}, nil
	}
	// Immediately remove the gracefully paused time annotation if the composition is not gracefully paused.
	meta.RemoveAnnotations(mg, compositionMeta.AnnotationKeyReconciliationGracefullyPausedTime)
	mg, err = tools.Update(ctx, mg, updateOpts)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("updating cr with values: %w", err)
	}

	if h.packageInfoGetter == nil {
		return controller.ExternalObservation{}, fmt.Errorf("helm chart package info getter must be specified")
	}
	pkg, err := h.packageInfoGetter.WithLogger(log).Get(mg)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("getting package info: %w", err)
	}

	compositionMeta.SetCompositionDefinitionLabels(mg, compositionMeta.CompositionDefinitionInfo{
		Name:      pkg.CompositionDefinitionInfo.Name,
		Namespace: pkg.CompositionDefinitionInfo.Namespace,
		GVR:       pkg.CompositionDefinitionInfo.GVR,
	})
	// This sets the labels for the composition definition and release name
	mg, err = tools.Update(ctx, mg, updateOpts)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("updating cr with values: %w", err)
	}

	hc, err := helm.NewClient(h.kubeconfig,
		helm.WithNamespace(mg.GetNamespace()),
		helm.WithLogger(h.getHelmLogger(meta.IsVerbose(mg))),
		helm.WithCache(),
	)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("creating helm client: %w", err)
	}

	rel, err := hc.GetRelease(ctx, releaseName, &helmconfig.GetConfig{})
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("finding helm release: %w", err)
	}
	if rel == nil {
		log.Debug("Release not found.")
		return controller.ExternalObservation{
			ResourceExists:   false,
			ResourceUpToDate: false,
		}, nil
	}

	if rel.Status == helmconfig.StatusPendingInstall || rel.Status == helmconfig.StatusPendingUpgrade || rel.Status == helmconfig.StatusPendingRollback {
		log.Debug("Composition stuck install or upgrade in progress. Rolling back to previous release before re-attempting.")
		// Rollback to previous release
		rel, err = hc.Rollback(ctx, releaseName, &helmconfig.RollbackConfig{
			MaxHistory:     helmMaxHistory,
			ReleaseVersion: rel.Revision,
		})
		if err != nil {
			return controller.ExternalObservation{}, fmt.Errorf("rolling back release: %w", err)
		}
	}

	compositionGVR, err := h.pluralizer.GVKtoGVR(mg.GroupVersionKind())
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("converting GVK to GVR: %w", err)
	}

	chartInspector := chartinspector.NewChartInspector(h.chartInspectorUrl)
	rbgen := metrics.WrapRBACGen(rbacgen.NewRBACGen(h.saName, h.saNamespace, &chartInspector))
	// Get Resources and generate RBAC
	generated, err := rbgen.
		WithBaseName(releaseName).
		Generate(ctx, rbacgen.Parameters{
			CompositionName:                mg.GetName(),
			CompositionNamespace:           mg.GetNamespace(),
			CompositionGVR:                 compositionGVR,
			CompositionDefinitionName:      pkg.CompositionDefinitionInfo.Name,
			CompositionDefinitionNamespace: pkg.CompositionDefinitionInfo.Namespace,
			CompositionDefintionGVR:        pkg.CompositionDefinitionInfo.GVR,
		})
	if err != nil {
		retErr := fmt.Errorf("generating RBAC using chart-inspector: %w", err)
		condition := condition.Unavailable()
		condition.Message = retErr.Error()
		unstructuredtools.SetConditions(mg, condition)
		_, err = tools.UpdateStatus(ctx, mg, updateOpts)
		if err != nil {
			return controller.ExternalObservation{}, fmt.Errorf("updating status after failure: %w", err)
		}
		return controller.ExternalObservation{}, fmt.Errorf("generating RBAC using chart-inspector: %w", retErr)
	}
	rbInstaller := rbac.NewRBACInstaller(dyn)
	helmMetrics := metrics.NewHelmMetrics(ctx)
	err = helmMetrics.TimedRBAC(func() error {
		return rbInstaller.ApplyRBAC(generated)
	})
	if err != nil {
		retErr := fmt.Errorf("applying rbac: %w", err)
		condition := condition.Unavailable()
		condition.Message = retErr.Error()
		unstructuredtools.SetConditions(mg, condition)
		_, err = tools.UpdateStatus(ctx, mg, updateOpts)
		if err != nil {
			return controller.ExternalObservation{}, fmt.Errorf("updating status after failure: %w", err)
		}
		return controller.ExternalObservation{}, retErr
	}

	tracer := tracer.NewTracer(ctx, meta.IsVerbose(mg))
	cfg := rest.CopyConfig(h.kubeconfig)
	cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		return tracer.WithRoundTripper(rt)
	}
	hc, err = helm.NewClient(cfg,
		helm.WithNamespace(mg.GetNamespace()),
		helm.WithCache(),
	)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("getting helm client: %w", err)
	}

	values, err := helmutils.ValuesFromSpec(mg)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("getting spec values: %w", err)
	}
	err = values.InjectGlobalValues(mg, h.pluralizer, krateoNamespace)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("injecting global values: %w", err)
	}
	postrenderLabels, err := utils.LabelPostRenderFromSpec(mg, h.pluralizer, krateoNamespace)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("creating label post renderer: %w", err)
	}
	// Stamp the active reconcile trace onto every child manifest (krateo.io/traceparent) so the
	// child composition's controller continues the same distributed trace. No-op when tracing is
	// off; excluded from the release digest (processor.ComputeReleaseDigest) so it never churns.
	tpCarrier := map[string]string{}
	telemetry.InjectTraceparent(ctx, tpCarrier)
	postrenderLabels.WithTraceparent(tpCarrier[meta.AnnotationKeyTraceparent], tpCarrier[meta.AnnotationKeyTracestate])
	helmMetrics = metrics.NewHelmMetrics(ctx)
	// Observe is READ-ONLY: render the upgrade as a server-side dry-run so we obtain the manifest
	// that a real upgrade WOULD apply (post-renderer runs, templates resolve, the API server
	// validates) and can compute its digest, but WITHOUT persisting a new helm revision. Previously
	// this was an unconditional live Upgrade every reconcile (~60s), which created an identical
	// revision every cycle even at steady state — infinite revision churn and needless API-server
	// load (upstream krateoplatformops/composition-dynamic-controller#184). The actual mutation is
	// now performed only when drift is detected: Observe returns ResourceUpToDate:false and the
	// runtime routes to Update (or Create for a missing release), which runs the live Upgrade.
	upgradedRel, err := helmMetrics.TimedUpgradeWithResult(func() (*helmconfig.Release, error) {
		return hc.Upgrade(ctx, releaseName, pkg.URL, &helmconfig.UpgradeConfig{
			ActionConfig: &helmconfig.ActionConfig{
				ChartVersion:          pkg.Version,
				ChartName:             pkg.Repo,
				Username:              pkg.Auth.Username,
				Password:              pkg.Auth.Password,
				InsecureSkipTLSverify: pkg.InsecureSkipTLSverify,
				Values:                values,
				PostRenderer:          postrenderLabels,
				// Adopt an existing child object rather than aborting the whole release when it carries
				// non-Helm ownership metadata (e.g. a composition instance created/edited out-of-band).
				// Without this, one un-adoptable child 500s the entire reconcile ("cannot be imported
				// into the current release: invalid ownership metadata") and wedges the platform (D1,
				// 2026-07-08); with it the release takes ownership, self-healing the conflict.
				TakeOwnership:         true,
				// Server-side dry-run: render + validate against the live cluster without writing a
				// revision. DryRunServer (not DryRunClient) so CRD-backed children and lookups are
				// validated exactly as a real upgrade would.
				DryRun:                helmconfig.DryRunServer,
			},
			MaxHistory: helmMaxHistory,
		})
	})
	if err != nil {
		retErr := fmt.Errorf("rendering helm chart (dry-run): %w", err)
		condition := condition.Unavailable()
		condition.Message = retErr.Error()
		unstructuredtools.SetConditions(mg, condition)
		_, err = tools.UpdateStatus(ctx, mg, updateOpts)
		if err != nil {
			return controller.ExternalObservation{}, fmt.Errorf("updating status after failure: %w", err)
		}
		return controller.ExternalObservation{}, retErr
	}

	digest, err := processor.ComputeReleaseDigest(upgradedRel)
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("computing release digest: %w", err)
	}

	previousDigest, err := maps.NestedString(mg.Object, "status", "digest")
	if err != nil {
		return controller.ExternalObservation{}, fmt.Errorf("getting previous digest from status: %w", err)
	}
	if previousDigest == "" {
		// Calculate the digest from the previous release if not present in status
		log.Debug("Previous digest not found in status, calculating from previous release")
		previousDigest, err = processor.ComputeReleaseDigest(rel)
		if err != nil {
			return controller.ExternalObservation{}, fmt.Errorf("computing release digest from previous release: %w", err)
		}
	}
	if digest != previousDigest {
		log.Debug("Composition out-of-date.", "package", pkg.URL, "current", digest, "expected", previousDigest)
		return controller.ExternalObservation{
			ResourceExists:   true,
			ResourceUpToDate: false,
		}, nil
	}

	if rel.ChartVersion != upgradedRel.ChartVersion {
		log.Debug("Composition package version mismatch.", "package", pkg.URL, "installed", rel.ChartVersion, "expected", pkg.Version)
		return controller.ExternalObservation{
			ResourceExists:   true,
			ResourceUpToDate: false,
		}, nil
	}

	err = h.setStatus(ctx, mg, &statusManagerOpts{
		force:           false,
		resources:       nil, // we don't need to set resources here as they are already set when a resource is created/updated
		previousDigest:  previousDigest,
		digest:          digest,
		message:         "Composition is up-to-date",
		chartURL:        pkg.URL,
		chartVersion:    pkg.Version,
		releaseStatus:   string(rel.Status),
		releaseRevision: rel.Revision,
		releaseName:     rel.Name,
		conditionType:   ConditionTypeAvailable,
	})
	if err != nil {
		return controller.ExternalObservation{}, err
	}

	_, err = tools.UpdateStatus(ctx, mg, updateOpts)
	if err != nil {
		return controller.ExternalObservation{}, err
	}

	log.Debug("Composition Observed - installed", "package", pkg.URL)

	return controller.ExternalObservation{
		ResourceExists:   true,
		ResourceUpToDate: true,
	}, nil
}

func (h *handler) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	mg = mg.DeepCopy()

	log := xcontext.Logger(ctx)
	log = log.WithValues("op", "Create").
		WithValues("apiVersion", mg.GetAPIVersion()).
		WithValues("kind", mg.GetKind()).
		WithValues("name", mg.GetName()).
		WithValues("namespace", mg.GetNamespace())

	dyn, err := dynamic.NewForConfig(h.kubeconfig)
	if err != nil {
		return fmt.Errorf("creating dynamic client: %w", err)
	}

	updateOpts := tools.UpdateOptions{
		Pluralizer:    h.pluralizer,
		DynamicClient: dyn,
	}

	if _, p := compositionMeta.GetGracefullyPausedTime(mg); p && compositionMeta.IsGracefullyPaused(mg) {
		log.Debug("Composition is gracefully paused, skipping create.")
		h.eventRecorder.Event(mg, event.Normal(reasonReconciliationGracefullyPaused, "Update", "Reconciliation is paused via the gracefully paused annotation."))
		return nil
	}

	compositionMeta.SetReleaseName(mg, compositionMeta.CalculateReleaseName(mg, h.safeReleaseName))
	releaseName := compositionMeta.GetReleaseName(mg)
	mg, err = tools.Update(ctx, mg, updateOpts)
	if err != nil {
		return fmt.Errorf("updating cr with values: %w", err)
	}

	if h.packageInfoGetter == nil {
		return fmt.Errorf("helm chart package info getter must be specified")
	}

	pkg, err := h.packageInfoGetter.WithLogger(log).Get(mg)
	if err != nil {
		return fmt.Errorf("getting package info: %w", err)
	}
	compositionGVR, err := h.pluralizer.GVKtoGVR(mg.GroupVersionKind())
	if err != nil {
		return fmt.Errorf("converting GVK to GVR: %w", err)
	}

	chartInspector := chartinspector.NewChartInspector(h.chartInspectorUrl)
	rbgen := metrics.WrapRBACGen(rbacgen.NewRBACGen(h.saName, h.saNamespace, &chartInspector))
	// Get Resources and generate RBAC
	generated, err := rbgen.
		WithBaseName(releaseName).
		Generate(ctx, rbacgen.Parameters{
			CompositionName:                mg.GetName(),
			CompositionNamespace:           mg.GetNamespace(),
			CompositionGVR:                 compositionGVR,
			CompositionDefinitionName:      pkg.CompositionDefinitionInfo.Name,
			CompositionDefinitionNamespace: pkg.CompositionDefinitionInfo.Namespace,
			CompositionDefintionGVR:        pkg.CompositionDefinitionInfo.GVR,
		})
	if err != nil {
		return fmt.Errorf("generating RBAC using chart-inspector: %w", err)
	}
	rbInstaller := rbac.NewRBACInstaller(dyn)
	helmMetrics := metrics.NewHelmMetrics(ctx)
	err = helmMetrics.TimedRBAC(func() error {
		return rbInstaller.ApplyRBAC(generated)
	})
	if err != nil {
		return fmt.Errorf("installing rbac: %w", err)
	}

	hc, err := helm.NewClient(h.kubeconfig,
		helm.WithNamespace(mg.GetNamespace()),
		helm.WithLogger(h.getHelmLogger(meta.IsVerbose(mg))),
	)
	if err != nil {
		return fmt.Errorf("creating helm client: %w", err)
	}

	values, err := helmutils.ValuesFromSpec(mg)
	if err != nil {
		return fmt.Errorf("getting spec values: %w", err)
	}
	err = values.InjectGlobalValues(mg, h.pluralizer, krateoNamespace)
	if err != nil {
		return fmt.Errorf("injecting global values: %w", err)
	}
	postrenderLabels, err := utils.LabelPostRenderFromSpec(mg, h.pluralizer, krateoNamespace)
	if err != nil {
		return fmt.Errorf("creating label post renderer: %w", err)
	}
	// Cross-composition trace propagation (see Observe); excluded from the release digest.
	tpCarrier := map[string]string{}
	telemetry.InjectTraceparent(ctx, tpCarrier)
	postrenderLabels.WithTraceparent(tpCarrier[meta.AnnotationKeyTraceparent], tpCarrier[meta.AnnotationKeyTracestate])

	actionConfig := &helmconfig.ActionConfig{
		ChartVersion:          pkg.Version,
		ChartName:             pkg.Repo,
		Values:                values,
		Username:              pkg.Auth.Username,
		Password:              pkg.Auth.Password,
		InsecureSkipTLSverify: pkg.InsecureSkipTLSverify,
		PostRenderer:          postrenderLabels,
		// Adopt an existing child object rather than aborting the whole release when it carries
		// non-Helm ownership metadata (out-of-band-created/edited composition instance). Otherwise one
		// un-adoptable child 500s the entire reconcile and wedges the platform (D1); with it the
		// release takes ownership and self-heals the conflict.
		TakeOwnership:         true,
	}

	// Check if the release already exists before attempting to install, this can happen if the create event is triggered after a failed install
	rel, err := hc.GetRelease(ctx, releaseName, &helmconfig.GetConfig{})
	if err != nil {
		return fmt.Errorf("finding helm release: %w", err)
	}
	helmMetrics = metrics.NewHelmMetrics(ctx)
	if rel != nil {
		log.Debug("Release already exists, upgrading instead of installing.")
		rel, err = helmMetrics.TimedUpgradeWithResult(func() (*helmconfig.Release, error) {
			return hc.Upgrade(ctx, releaseName, pkg.URL, &helmconfig.UpgradeConfig{
				ActionConfig: actionConfig,
				MaxHistory:   helmMaxHistory,
			})
		})
	} else {
		rel, err = helmMetrics.TimedInstallWithResult(func() (*helmconfig.Release, error) {
			return hc.Install(ctx, releaseName, pkg.URL, &helmconfig.InstallConfig{
				ActionConfig: actionConfig,
			})
		})
		if err != nil {
			return fmt.Errorf("installing helm chart: %w", err)
		}
	}

	log.Debug("Installing composition package", "package", pkg.URL)

	all, digest, err := processor.DecodeMinRelease(rel)
	if err != nil {
		return fmt.Errorf("decoding release: %w", err)
	}

	err = h.setStatus(ctx, mg, &statusManagerOpts{
		force:           true,
		resources:       all,
		previousDigest:  "",
		digest:          digest,
		message:         "Composition created",
		chartURL:        pkg.URL,
		chartVersion:    pkg.Version,
		releaseStatus:   string(rel.Status),
		releaseRevision: rel.Revision,
		releaseName:     rel.Name,
		conditionType:   ConditionTypeAvailable,
	})
	if err != nil {
		return fmt.Errorf("setting status: %w", err)
	}

	log.Debug("Composition created.", "package", pkg.URL)

	h.eventRecorder.Event(mg, event.Normal(reasonCreated, "Create", fmt.Sprintf("Composition created: %s", mg.GetName())))
	mg, err = tools.UpdateStatus(ctx, mg, updateOpts)
	if err != nil {
		return fmt.Errorf("updating cr with values: %w", err)
	}

	meta.RemoveAnnotations(mg, compositionMeta.AnnotationKeyReconciliationGracefullyPausedTime)
	_, err = tools.Update(ctx, mg, updateOpts)
	if err != nil {
		return fmt.Errorf("updating cr with values: %w", err)
	}

	return nil
}

func (h *handler) Update(ctx context.Context, mg *unstructured.Unstructured) error {
	mg = mg.DeepCopy()
	releaseName := compositionMeta.GetReleaseName(mg)

	log := xcontext.Logger(ctx)

	log = log.WithValues("op", "Update").
		WithValues("apiVersion", mg.GetAPIVersion()).
		WithValues("kind", mg.GetKind()).
		WithValues("name", mg.GetName()).
		WithValues("namespace", mg.GetNamespace())

	dyn, err := dynamic.NewForConfig(h.kubeconfig)
	if err != nil {
		return fmt.Errorf("creating dynamic client: %w", err)
	}

	updateOpts := tools.UpdateOptions{
		Pluralizer:    h.pluralizer,
		DynamicClient: dyn,
	}

	if _, p := compositionMeta.GetGracefullyPausedTime(mg); p && compositionMeta.IsGracefullyPaused(mg) {
		log.Debug("Composition is gracefully paused, skipping update.")
		h.eventRecorder.Event(mg, event.Normal(reasonReconciliationGracefullyPaused, "Update", "Reconciliation is paused via the gracefully paused annotation."))
		return nil
	}

	log.Debug("Handling composition update")

	if h.packageInfoGetter == nil {
		return fmt.Errorf("helm chart package info getter must be specified")
	}

	pkg, err := h.packageInfoGetter.WithLogger(log).Get(mg)
	if err != nil {
		return fmt.Errorf("getting package info: %w", err)
	}

	// Update the helm chart. Observe now renders as a dry-run (no revision written), so the live
	// mutation happens HERE — the runtime routes to Update only when Observe reported drift. Perform
	// the real Upgrade so the change is actually applied, mirroring the Observe render (same values,
	// global-value injection, post-render labels + traceparent, TakeOwnership).
	hc, err := helm.NewClient(h.kubeconfig,
		helm.WithNamespace(mg.GetNamespace()),
		helm.WithLogger(h.getHelmLogger(meta.IsVerbose(mg))),
	)
	if err != nil {
		return fmt.Errorf("creating helm client: %w", err)
	}

	values, err := helmutils.ValuesFromSpec(mg)
	if err != nil {
		return fmt.Errorf("getting spec values: %w", err)
	}
	err = values.InjectGlobalValues(mg, h.pluralizer, krateoNamespace)
	if err != nil {
		return fmt.Errorf("injecting global values: %w", err)
	}
	postrenderLabels, err := utils.LabelPostRenderFromSpec(mg, h.pluralizer, krateoNamespace)
	if err != nil {
		return fmt.Errorf("creating label post renderer: %w", err)
	}
	// Cross-composition trace propagation (see Observe); excluded from the release digest.
	tpCarrier := map[string]string{}
	telemetry.InjectTraceparent(ctx, tpCarrier)
	postrenderLabels.WithTraceparent(tpCarrier[meta.AnnotationKeyTraceparent], tpCarrier[meta.AnnotationKeyTracestate])

	helmMetrics := metrics.NewHelmMetrics(ctx)
	upgradedRel, err := helmMetrics.TimedUpgradeWithResult(func() (*helmconfig.Release, error) {
		return hc.Upgrade(ctx, releaseName, pkg.URL, &helmconfig.UpgradeConfig{
			ActionConfig: &helmconfig.ActionConfig{
				ChartVersion:          pkg.Version,
				ChartName:             pkg.Repo,
				Username:              pkg.Auth.Username,
				Password:              pkg.Auth.Password,
				InsecureSkipTLSverify: pkg.InsecureSkipTLSverify,
				Values:                values,
				PostRenderer:          postrenderLabels,
				TakeOwnership:         true,
			},
			MaxHistory: helmMaxHistory,
		})
	})
	if err != nil {
		return fmt.Errorf("upgrading helm chart: %w", err)
	}
	if upgradedRel == nil {
		log.Debug("Release not found after upgrade.")
		return fmt.Errorf("release not found after upgrade")
	}

	previousDigest, err := maps.NestedString(mg.Object, "status", "digest")
	if err != nil {
		return fmt.Errorf("getting previous digest from status: %w", err)
	}

	all, digest, err := processor.DecodeMinRelease(upgradedRel)
	if err != nil {
		return fmt.Errorf("decoding release: %w", err)
	}

	managed, err := h.populateManagedResources(all)
	if err != nil {
		return fmt.Errorf("populating managed resources: %w", err)
	}
	setManagedResources(mg, managed)

	log.Debug("Composition values updated.", "package", pkg.URL)

	h.eventRecorder.Event(mg, event.Normal(reasonUpdated, "Update", fmt.Sprintf("Updated composition: %s", mg.GetName())))

	statusOpts := &statusManagerOpts{
		force:           false,
		resources:       all,
		digest:          digest,
		previousDigest:  previousDigest,
		message:         "Composition values updated",
		chartURL:        pkg.URL,
		chartVersion:    pkg.Version,
		releaseStatus:   string(upgradedRel.Status),
		releaseRevision: upgradedRel.Revision,
		releaseName:     upgradedRel.Name,
		conditionType:   ConditionTypeAvailable,
	}
	err = h.setStatus(ctx, mg, statusOpts)
	if err != nil {
		return fmt.Errorf("setting status: %w", err)
	}

	mg, err = tools.UpdateStatus(ctx, mg, tools.UpdateOptions{
		Pluralizer:    h.pluralizer,
		DynamicClient: dyn,
	})
	if err != nil {
		return fmt.Errorf("updating cr status with values: %w", err)
	}

	if compositionMeta.IsGracefullyPaused(mg) {
		statusOpts.conditionType = ConditionTypeReconcileGracefullyPaused
		compositionMeta.SetGracefullyPausedTime(mg, time.Now())
		log.Debug("Composition gracefully paused.")
		h.eventRecorder.Event(mg, event.Normal(reasonReconciliationGracefullyPaused, "Update", "Reconciliation paused via the gracefully paused annotation."))

	} else {
		statusOpts.conditionType = ConditionTypeAvailable
		meta.RemoveAnnotations(mg, compositionMeta.AnnotationKeyReconciliationGracefullyPausedTime)
	}

	mg, err = tools.Update(ctx, mg, updateOpts)
	if err != nil {
		return fmt.Errorf("updating cr with values: %w", err)
	}
	return nil
}

func (h *handler) Delete(ctx context.Context, mg *unstructured.Unstructured) error {
	mg = mg.DeepCopy()

	releaseName := compositionMeta.GetReleaseName(mg)

	log := xcontext.Logger(ctx)

	log = log.WithValues("op", "Delete").
		WithValues("apiVersion", mg.GetAPIVersion()).
		WithValues("kind", mg.GetKind()).
		WithValues("name", mg.GetName()).
		WithValues("namespace", mg.GetNamespace())

	dyn, err := dynamic.NewForConfig(h.kubeconfig)
	if err != nil {
		return fmt.Errorf("creating dynamic client: %w", err)
	}

	updateOpts := tools.UpdateOptions{
		Pluralizer:    h.pluralizer,
		DynamicClient: dyn,
	}

	if _, p := compositionMeta.GetGracefullyPausedTime(mg); p && compositionMeta.IsGracefullyPaused(mg) {
		log.Debug("Composition is gracefully paused, skipping delete.")
		h.eventRecorder.Event(mg, event.Normal(reasonReconciliationGracefullyPaused, "Delete", "Reconciliation is paused via the gracefully paused annotation."))
		return nil
	}

	if h.packageInfoGetter == nil {
		return fmt.Errorf("helm chart package info getter must be specified")
	}

	hc, err := helm.NewClient(h.kubeconfig,
		helm.WithNamespace(mg.GetNamespace()),
		helm.WithLogger(h.getHelmLogger(meta.IsVerbose(mg))),
	)
	if err != nil {
		return fmt.Errorf("creating helm client: %w", err)
	}

	pkg, err := h.packageInfoGetter.WithLogger(log).Get(mg)
	if err != nil {
		return fmt.Errorf("getting package info: %w", err)
	}

	// Check if the release exists before uninstalling
	rel, err := hc.GetRelease(ctx, releaseName, &helmconfig.GetConfig{})
	if err != nil {
		return fmt.Errorf("finding helm release: %w", err)
	}
	if rel == nil {
		log.Debug("Release not found, nothing to uninstall.", "package", pkg.URL)
		h.eventRecorder.Event(mg, event.Normal(reasonDeleted, "Delete", fmt.Sprintf("Release not found, nothing to uninstall: %s", mg.GetName())))
		return nil
	}

	helmMetrics := metrics.NewHelmMetrics(ctx)
	err = helmMetrics.TimedUninstall(func() error {
		return hc.Uninstall(ctx, releaseName, &helmconfig.UninstallConfig{
			IgnoreNotFound: true,
		})
	})
	if err != nil {
		return fmt.Errorf("uninstalling helm chart: %w", err)
	}

	rel, err = hc.GetRelease(ctx, releaseName, &helmconfig.GetConfig{})
	if err != nil {
		return fmt.Errorf("finding helm release: %w", err)
	}
	if rel != nil {
		return fmt.Errorf("composition not deleted, release %s still exists", releaseName)
	}

	log.Debug("Uninstalling RBAC", "package", pkg.URL)

	compositionGVR, err := h.pluralizer.GVKtoGVR(mg.GroupVersionKind())
	if err != nil {
		return fmt.Errorf("converting GVK to GVR: %w", err)
	}
	chartInspector := chartinspector.NewChartInspector(h.chartInspectorUrl)
	rbgen := metrics.WrapRBACGen(rbacgen.NewRBACGen(h.saName, h.saNamespace, &chartInspector))

	// Get Resources and generate RBAC
	generated, err := rbgen.
		WithBaseName(compositionMeta.GetReleaseName(mg)).
		Generate(ctx, rbacgen.Parameters{
			CompositionName:                mg.GetName(),
			CompositionNamespace:           mg.GetNamespace(),
			CompositionGVR:                 compositionGVR,
			CompositionDefinitionName:      pkg.CompositionDefinitionInfo.Name,
			CompositionDefinitionNamespace: pkg.CompositionDefinitionInfo.Namespace,
			CompositionDefintionGVR:        pkg.CompositionDefinitionInfo.GVR,
		})
	if err != nil {
		return fmt.Errorf("generating RBAC for composition %s/%s: %w",
			mg.GetNamespace(), mg.GetName(), err)
	}
	rbInstaller := rbac.NewRBACInstaller(dyn)
	err = helmMetrics.TimedRBAC(func() error {
		return rbInstaller.UninstallRBAC(generated)
	})
	if err != nil {
		return fmt.Errorf("uninstalling rbac: %w", err)
	}

	h.eventRecorder.Event(mg, event.Normal(reasonDeleted, "Delete", fmt.Sprintf("Deleted composition: %s", mg.GetName())))
	log.Debug("Composition package removed.", "package", pkg.URL)
	meta.RemoveAnnotations(mg, compositionMeta.AnnotationKeyReconciliationGracefullyPausedTime)

	_, err = tools.Update(ctx, mg, updateOpts)
	if err != nil {
		return fmt.Errorf("updating cr with values: %w", err)
	}

	return nil
}

func (h *handler) getHelmLogger(verbose bool) func(format string, v ...interface{}) {
	if verbose {
		return func(format string, v ...interface{}) {
			slog.Debug(fmt.Sprintf(format, v...))
		}
	}
	return func(format string, v ...interface{}) {}
}
