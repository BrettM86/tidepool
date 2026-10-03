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
}

// Reconciler compares bridged instance labels with Cloudflare NS records and
// creates missing delegations without modifying existing records.
type Reconciler struct {
	client        *CloudflareClient
	labels        LabelSource
	contributions ContributionSource
	acceptances   AcceptanceChecker
	now           func() time.Time
	zoneRoot      string
	nameservers   []string
	logger        *slog.Logger
}

func normalizeDNSName(name string) string {
	return strings.TrimRight(strings.ToLower(name), ".")
}

// NewReconciler validates options and normalizes the zone and nameservers.
func NewReconciler(options Options) (*Reconciler, error) {
	if options.Client == nil || options.Labels == nil || options.Contributions == nil || options.Acceptances == nil || normalizeDNSName(options.ZoneRoot) == "" || len(options.Nameservers) == 0 {
		return nil, fmt.Errorf("delegation client, label source, contribution source, acceptance checker, zone root and nameservers are required")
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
	return &Reconciler{
		client: options.Client, labels: options.Labels, zoneRoot: normalizeDNSName(options.ZoneRoot),
		contributions: options.Contributions, acceptances: options.Acceptances, now: now,
		nameservers: nameservers, logger: logger,
	}, nil
}

// Result summarizes a completed pass. Labels in every field are bare,
// lowercased DNS labels rather than fully qualified names.
type Result struct {
	// Created contains live labels for which an NS record was created.
	Created []string
	// AlreadyDelegated contains live labels with a configured NS record.
	AlreadyDelegated []string
	// Conflicting contains labels with foreign NS records, including labels
	// that also have a configured NS record.
	Conflicting []Conflict
	// Failed contains live labels whose qualification or NS creation failed.
	Failed []Failure
	// DelegatedWithoutLiveActors contains labels with configured NS records
	// but no live actors and no foreign NS records.
	DelegatedWithoutLiveActors []string
	// Pending contains live labels without a standing contribution or outright qualification.
	Pending []string
}

// Conflict identifies a label and the foreign nameservers in its NS records.
type Conflict struct {
	Label    string
	Contents []string
}

// Failure identifies a label whose NS record could not be created and why.
type Failure struct {
	Label  string
	Reason string
}

// Reconcile lists labels and zone NS records, qualifies eligible live labels,
// then creates their missing delegations. Per-label failures do not stop the pass.
func (r *Reconciler) Reconcile(ctx context.Context) (Result, error) {
	var result Result
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
	type delegationState struct {
		delegated bool
		foreign   []string
	}
	delegations := make(map[string]*delegationState)
	for _, record := range records {
		if !strings.EqualFold(record.Type, "NS") {
			continue
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
	// Keep the qualification timestamp internally for task 08's ordering.
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
	cutoff := r.now().Add(-minimumStandingAge)
	var failures []error
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
		if err := r.client.createRecord(ctx, label+"."+r.zoneRoot, r.nameservers[0]); err != nil {
			result.Failed = append(result.Failed, Failure{Label: label, Reason: err.Error()})
			failures = append(failures, err)
			r.logger.ErrorContext(ctx, "create NS delegation failed", "label", label, "error", err)
			continue
		}
		result.Created = append(result.Created, label)
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
