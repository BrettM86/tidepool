package delegation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	"tidepool/internal/store"
)

// LabelSource supplies distinct instance labels and their live-actor status.
type LabelSource interface {
	ListInstanceLabels(ctx context.Context, zoneRoot string) ([]store.InstanceLabel, error)
}

// Options configures a delegation reconciler for a DNS zone.
type Options struct {
	Client      *CloudflareClient
	Labels      LabelSource
	ZoneRoot    string
	Nameservers []string
	Logger      *slog.Logger
}

// Reconciler compares bridged instance labels with Cloudflare NS records and
// creates missing delegations without modifying existing records.
type Reconciler struct {
	client      *CloudflareClient
	labels      LabelSource
	zoneRoot    string
	nameservers []string
	logger      *slog.Logger
}

func normalizeDNSName(name string) string {
	return strings.TrimRight(strings.ToLower(name), ".")
}

// NewReconciler validates options and normalizes the zone and nameservers.
func NewReconciler(options Options) (*Reconciler, error) {
	if options.Client == nil || options.Labels == nil || normalizeDNSName(options.ZoneRoot) == "" || len(options.Nameservers) == 0 {
		return nil, fmt.Errorf("delegation client, label source, zone root and nameservers are required")
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
	return &Reconciler{
		client: options.Client, labels: options.Labels, zoneRoot: normalizeDNSName(options.ZoneRoot),
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
	// Failed contains live labels whose NS record could not be created.
	Failed []Failure
	// DelegatedWithoutLiveActors contains labels with configured NS records
	// but no live actors and no foreign NS records.
	DelegatedWithoutLiveActors []string
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

// Reconcile lists all instance labels and zone NS records, then creates only
// the missing delegations for live labels. It reports create failures after
// attempting the remaining labels.
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
		if err := r.client.createRecord(ctx, label+"."+r.zoneRoot, r.nameservers[0]); err != nil {
			result.Failed = append(result.Failed, Failure{Label: label, Reason: err.Error()})
			failures = append(failures, err)
			r.logger.ErrorContext(ctx, "create NS delegation failed", "label", label, "error", err)
			continue
		}
		result.Created = append(result.Created, label)
	}
	r.logger.InfoContext(ctx, "delegation pass complete", "created", len(result.Created), "already_delegated", len(result.AlreadyDelegated),
		"conflicting", len(result.Conflicting), "failed", len(result.Failed), "delegated_without_live_actors", len(result.DelegatedWithoutLiveActors))
	return result, errors.Join(failures...)
}
