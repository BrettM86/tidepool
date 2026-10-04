package delegation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"tidepool/internal/store"
)

// LabelSource supplies distinct instance labels and their live-actor status.
type LabelSource interface {
	ListInstanceLabels(ctx context.Context, zoneRoot string) ([]store.InstanceLabel, error)
}

const (
	minimumStandingAge            = time.Hour
	contributionCandidatePageSize = 100
	maximumCreatesPerHour         = 20
	budgetWindow                  = time.Hour
	ceilingWarningPercent         = 80
)

var grandfatherCutoff = time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)

// ContributionSource lists candidates and outright qualifications for live labels.
type ContributionSource interface {
	ListLabelContributions(ctx context.Context, zoneRoot, label string, cutoff time.Time, after store.ContributionCursor, limit int) ([]store.Contribution, error)
	ListOutrightQualifiedLabels(ctx context.Context, zoneRoot string, labels []string, grandfatherCutoff time.Time) ([]store.OutrightQualification, error)
}

// AcceptanceChecker checks whether a postv2's community acceptance still stands.
type AcceptanceChecker interface {
	AcceptanceStands(ctx context.Context, communityDID, subjectURI string) (bool, error)
}

// Options configures a delegation reconciler for a DNS zone.
type Options struct {
	Client        *CloudflareClient
	Labels        LabelSource
	ZoneRoot      string
	Nameservers   []string
	Logger        *slog.Logger
	Contributions ContributionSource
	Acceptances   AcceptanceChecker
	Now           func() time.Time
	MaxNSRecords  int
	Interval      time.Duration
}

// ErrCeilingReached reports that the NS record ceiling stopped a pass with qualifying labels left.
var ErrCeilingReached = errors.New("delegation NS record ceiling reached")

// Reconciler compares bridged instance labels with Cloudflare NS records and
// creates missing delegations without modifying existing records, within an
// NS record ceiling and an hourly create budget counted across the whole zone.
type Reconciler struct {
	client        *CloudflareClient
	labels        LabelSource
	contributions ContributionSource
	acceptances   AcceptanceChecker
	now           func() time.Time
	zoneRoot      string
	nameservers   []string
	logger        *slog.Logger
	maxNSRecords  int
	interval      time.Duration
	passSlot      chan struct{}
}

// Run reconciles immediately and at each configured interval until ctx is done.
func (r *Reconciler) Run(ctx context.Context) {
	pass := func() {
		result, err := r.Reconcile(ctx)
		r.LogPass(ctx, result, err)
	}

	if ctx.Err() != nil {
		return
	}
	pass()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			pass()
		}
	}
}

// LogPass logs a pass's summary and error the way Run does; a pass ended by
// ctx's own cancellation logs nothing.
func (r *Reconciler) LogPass(ctx context.Context, result Result, err error) {
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return
	}
	r.logger.InfoContext(ctx, "delegation pass finished", "component", "delegation",
		"created_count", len(result.Created), "created", result.Created,
		"already_delegated_count", len(result.AlreadyDelegated),
		"conflicting_count", len(result.Conflicting), "conflicting", result.Conflicting,
		"pending_count", len(result.Pending), "pending", result.Pending,
		"deferred_count", len(result.Deferred), "delegated_records", result.DelegatedRecords, "ceiling", result.Ceiling,
		"failed_count", len(result.Failed), "failed", result.Failed,
		"delegated_without_live_actors_count", len(result.DelegatedWithoutLiveActors))
	if err != nil {
		r.logger.ErrorContext(ctx, "delegation pass failed", "component", "delegation", "error", err)
	}
}

func normalizeDNSName(name string) string {
	return strings.TrimRight(strings.ToLower(name), ".")
}

// NewReconciler validates options and normalizes the zone and nameservers.
func NewReconciler(options Options) (*Reconciler, error) {
	if options.Client == nil || options.Labels == nil || options.Contributions == nil || options.Acceptances == nil || normalizeDNSName(options.ZoneRoot) == "" || len(options.Nameservers) == 0 {
		return nil, fmt.Errorf("delegation client, label source, contribution source, acceptance checker, zone root and nameservers are required")
	}
	if options.MaxNSRecords <= 0 {
		return nil, fmt.Errorf("delegation MaxNSRecords must be positive")
	}
	nameservers := make([]string, len(options.Nameservers))
	for index, nameserver := range options.Nameservers {
		nameservers[index] = normalizeDNSName(nameserver)
		if nameservers[index] == "" {
			return nil, fmt.Errorf("delegation nameserver at index %d is empty", index)
		}
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	interval := options.Interval
	if interval == 0 {
		interval = 15 * time.Minute
	}
	return &Reconciler{
		client: options.Client, labels: options.Labels, zoneRoot: normalizeDNSName(options.ZoneRoot),
		contributions: options.Contributions, acceptances: options.Acceptances, now: now,
		nameservers: nameservers, logger: logger, maxNSRecords: options.MaxNSRecords,
		interval: interval, passSlot: make(chan struct{}, 1),
	}, nil
}

// Result summarizes a completed pass. Labels in every field are bare,
// lowercased DNS labels rather than fully qualified names.
type Result struct {
	// Created contains live labels for which an NS record was created.
	Created []string `json:"created"`
	// AlreadyDelegated contains live labels with a configured NS record.
	AlreadyDelegated []string `json:"already_delegated"`
	// Conflicting contains labels with foreign NS records, including labels
	// that also have a configured NS record.
	Conflicting []Conflict `json:"conflicting"`
	// Failed contains live labels whose qualification or NS creation failed.
	Failed []Failure `json:"failed"`
	// DelegatedWithoutLiveActors contains labels with configured NS records
	// but no live actors and no foreign NS records.
	DelegatedWithoutLiveActors []string `json:"delegated_without_live_actors"`
	// Pending contains live labels without a standing contribution or outright qualification.
	Pending []string `json:"pending"`
	// Deferred contains qualifying labels left for a later pass by the ceiling or the hourly budget.
	Deferred []string `json:"deferred"`
	// DelegatedRecords counts configured-nameserver NS records at any name in
	// the zone, including names outside the bridge hostname, plus this pass's
	// successful creates.
	DelegatedRecords int `json:"delegated_records"`
	// Ceiling is the configured maximum number of configured-nameserver NS records.
	Ceiling int `json:"ceiling"`
}

// Conflict identifies a label and the foreign nameservers in its NS records.
type Conflict struct {
	Label    string   `json:"label"`
	Contents []string `json:"contents"`
}

// Failure identifies a label whose NS record could not be created and why.
type Failure struct {
	Label  string `json:"label"`
	Reason string `json:"reason"`
}

// Reconcile lists labels and zone NS records, qualifies eligible live labels,
// then creates their missing delegations oldest qualification first. Creates
// are limited by the NS record ceiling and the rolling-hour create budget;
// qualifying labels left over go to Deferred. When the ceiling stops the pass,
// the returned error wraps ErrCeilingReached (testable with errors.Is).
// Per-label failures do not stop the pass.
func (r *Reconciler) Reconcile(ctx context.Context) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	select {
	case r.passSlot <- struct{}{}:
		defer func() { <-r.passSlot }()
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	result := Result{
		Created: []string{}, AlreadyDelegated: []string{}, Conflicting: []Conflict{},
		Failed: []Failure{}, DelegatedWithoutLiveActors: []string{},
		Pending: []string{}, Deferred: []string{}, Ceiling: r.maxNSRecords,
	}
	labels, err := r.labels.ListInstanceLabels(ctx, r.zoneRoot)
	if err != nil {
		return result, fmt.Errorf("list instance labels for %s: %w", r.zoneRoot, err)
	}
	records, err := r.client.listRecords(ctx)
	if err != nil {
		return result, fmt.Errorf("list delegations for %s: %w", r.zoneRoot, err)
	}

	liveLabels := make(map[string]bool, len(labels))
	for _, label := range labels {
		name := normalizeDNSName(label.Label)
		liveLabels[name] = liveLabels[name] || label.HasLiveActor
	}
	configured := make(map[string]bool, len(r.nameservers))
	for _, nameserver := range r.nameservers {
		configured[nameserver] = true
	}
	now := r.now()
	recentRecords := 0
	type delegationState struct {
		delegated bool
		foreign   []string
	}
	delegations := make(map[string]*delegationState)
	for _, record := range records {
		if !strings.EqualFold(record.Type, "NS") {
			continue
		}
		// Both guards protect the per-zone Cloudflare quota, so they count
		// configured-nameserver NS records at any name in the zone.
		if configured[normalizeDNSName(record.Content)] {
			result.DelegatedRecords++
			if record.CreatedOn.After(now.Add(-budgetWindow)) {
				recentRecords++
			}
		}
		name := normalizeDNSName(record.Name)
		label, ok := strings.CutSuffix(name, "."+r.zoneRoot)
		if !ok || label == "" || strings.Contains(label, ".") {
			continue
		}
		state := delegations[label]
		if state == nil {
			state = &delegationState{}
			delegations[label] = state
		}
		content := normalizeDNSName(record.Content)
		if configured[content] {
			state.delegated = true
		} else {
			state.foreign = append(state.foreign, content)
		}
	}

	allLabels := make(map[string]bool, len(liveLabels)+len(delegations))
	for label := range liveLabels {
		allLabels[label] = true
	}
	for label := range delegations {
		allLabels[label] = true
	}
	orderedLabels := make([]string, 0, len(allLabels))
	for label := range allLabels {
		orderedLabels = append(orderedLabels, label)
	}
	sort.Strings(orderedLabels)
	var eligible []string
	for _, label := range orderedLabels {
		state := delegations[label]
		if liveLabels[label] && (state == nil || !state.delegated && len(state.foreign) == 0) {
			eligible = append(eligible, label)
		}
	}
	qualifiedAt := make(map[string]time.Time, len(eligible))
	if len(eligible) > 0 {
		outright, err := r.contributions.ListOutrightQualifiedLabels(ctx, r.zoneRoot, eligible, grandfatherCutoff)
		if err != nil {
			return result, fmt.Errorf("list outright qualified labels for %s: %w", r.zoneRoot, err)
		}
		for _, entry := range outright {
			qualifiedAt[entry.Label] = entry.QualifiedAt
		}
	}
	cutoff := now.Add(-minimumStandingAge)
	var failures []error
	var qualifying []string
	for _, label := range orderedLabels {
		state := delegations[label]
		if state != nil {
			if state.delegated && liveLabels[label] {
				result.AlreadyDelegated = append(result.AlreadyDelegated, label)
			}
			if len(state.foreign) != 0 {
				result.Conflicting = append(result.Conflicting, Conflict{Label: label, Contents: state.foreign})
				r.logger.WarnContext(ctx, "foreign NS delegation", "label", label, "contents", state.foreign)
				continue
			}
			if state.delegated {
				if !liveLabels[label] {
					result.DelegatedWithoutLiveActors = append(result.DelegatedWithoutLiveActors, label)
					r.logger.InfoContext(ctx, "delegation without live actors", "label", label)
				}
				continue
			}
		}
		if !liveLabels[label] {
			continue
		}
		if _, qualified := qualifiedAt[label]; !qualified {
			standingAt, stands, err := r.firstStandingContribution(ctx, label, cutoff)
			if err != nil {
				failure := fmt.Errorf("qualify label %s: %w", label, err)
				result.Failed = append(result.Failed, Failure{Label: label, Reason: failure.Error()})
				failures = append(failures, failure)
				r.logger.ErrorContext(ctx, "delegation qualification failed", "label", label, "error", failure)
				continue
			}
			if !stands {
				result.Pending = append(result.Pending, label)
				continue
			}
			qualifiedAt[label] = standingAt
		}
		qualifying = append(qualifying, label)
	}
	sort.Slice(qualifying, func(i, j int) bool {
		first, second := qualifying[i], qualifying[j]
		if qualifiedAt[first].Equal(qualifiedAt[second]) {
			return first < second
		}
		return qualifiedAt[first].Before(qualifiedAt[second])
	})
	remainingBudget := max(0, maximumCreatesPerHour-recentRecords)
	ceilingReached := false
	for index, label := range qualifying {
		if result.DelegatedRecords >= r.maxNSRecords {
			result.Deferred = append(result.Deferred, qualifying[index:]...)
			ceilingReached = true
			r.logger.ErrorContext(ctx, "delegation NS record ceiling reached", "ceiling", r.maxNSRecords, "deferred", len(result.Deferred))
			failures = append(failures, fmt.Errorf("ceiling %d: %w", r.maxNSRecords, ErrCeilingReached))
			break
		}
		if remainingBudget == 0 {
			result.Deferred = append(result.Deferred, qualifying[index:]...)
			r.logger.WarnContext(ctx, "delegation hourly create budget exhausted", "deferred", len(result.Deferred))
			break
		}
		remainingBudget--
		if err := r.client.createRecord(ctx, label+"."+r.zoneRoot, r.nameservers[0]); err != nil {
			result.Failed = append(result.Failed, Failure{Label: label, Reason: err.Error()})
			failures = append(failures, err)
			r.logger.ErrorContext(ctx, "create NS delegation failed", "label", label, "error", err)
			continue
		}
		result.Created = append(result.Created, label)
		result.DelegatedRecords++
	}
	if !ceilingReached && int64(result.DelegatedRecords)*100 >= int64(r.maxNSRecords)*ceilingWarningPercent {
		r.logger.WarnContext(ctx, "delegation NS record ceiling nearing", "records", result.DelegatedRecords, "ceiling", r.maxNSRecords)
	}
	r.logger.InfoContext(ctx, "delegation pass complete", "created", len(result.Created), "already_delegated", len(result.AlreadyDelegated),
		"conflicting", len(result.Conflicting), "failed", len(result.Failed), "pending", len(result.Pending),
		"delegated_without_live_actors", len(result.DelegatedWithoutLiveActors))
	return result, errors.Join(failures...)
}

// firstStandingContribution pages until a candidate stands or the source runs out.
func (r *Reconciler) firstStandingContribution(ctx context.Context, label string, cutoff time.Time) (time.Time, bool, error) {
	var cursor store.ContributionCursor
	for {
		candidates, err := r.contributions.ListLabelContributions(ctx, r.zoneRoot, label, cutoff, cursor, contributionCandidatePageSize)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("list contributions: %w", err)
		}
		for _, candidate := range candidates {
			stands, err := r.contributionStands(ctx, candidate)
			if err != nil {
				return time.Time{}, false, err
			}
			if stands {
				return candidate.IndexedAt, true, nil
			}
		}
		if len(candidates) < contributionCandidatePageSize {
			return time.Time{}, false, nil
		}
		last := candidates[len(candidates)-1]
		cursor = store.ContributionCursor{IndexedAt: last.IndexedAt, ID: last.ID}
	}
}

// contributionStands reports whether a candidate is still visible. A postv2
// needs its community's acceptance; a comment counts only while its thread
// root stands, so a comment under a postv2 root needs the root's acceptance in
// the root's community. Legacy posts and their comments have no acceptance.
func (r *Reconciler) contributionStands(ctx context.Context, candidate store.Contribution) (bool, error) {
	switch candidate.Collection {
	case "social.coves.community.post":
		return true, nil
	case "social.coves.community.postv2":
		return r.acceptanceStands(ctx, candidate.CommunityDID, candidate.ATURI)
	case "social.coves.community.comment":
		switch candidate.RootCollection {
		case "social.coves.community.post":
			return true, nil
		case "social.coves.community.postv2":
			return r.acceptanceStands(ctx, candidate.RootCommunityDID, candidate.RootATURI)
		}
	}
	return false, nil
}

func (r *Reconciler) acceptanceStands(ctx context.Context, communityDID, subjectURI string) (bool, error) {
	stands, err := r.acceptances.AcceptanceStands(ctx, communityDID, subjectURI)
	if err != nil {
		return false, fmt.Errorf("check acceptance for %s: %w", subjectURI, err)
	}
	return stands, nil
}
