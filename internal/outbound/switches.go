package outbound

import (
	"strings"
	"sync/atomic"
)

// ConfigSwitches is the config-backed Switches: the operator kill switches
// (decision 19). main builds one and hands the pointer to the Worker and the
// follow reconciler. Disabled, Dry and DisabledActors are static environment
// values. The host and community sets come from the follow list and are
// replaced whole by SetDisabledSets while workers read them.
//
// A blocked delivery is PARKED by the worker (stays pending, resumes when the
// switch clears) — never poisoned or cancelled. The zero value allows
// everything, so a deployment that sets no switches behaves exactly like
// AllowAll.
type ConfigSwitches struct {
	// Disabled is the global kill switch: when true, every delivery is blocked.
	Disabled bool
	// Dry, when true, makes the worker translate + log but POST nothing.
	Dry bool
	// DisabledActors blocks a delivery whose actor DID is in the set. A nil set
	// blocks nothing.
	DisabledActors map[string]struct{}

	// sets is nil until the first SetDisabledSets, which blocks nothing.
	sets atomic.Pointer[disabledSets]
}

// disabledSets is one generation of the swappable sets. A generation is never
// mutated once stored.
type disabledSets struct {
	hosts       map[string]struct{}
	communities map[string]struct{}
}

// OutboundAllowed reports whether a delivery in this scope may be sent: false
// when the global switch is engaged or the scope's host, community, or actor is
// in a disabled set.
func (s *ConfigSwitches) OutboundAllowed(scope DeliveryScope) bool {
	if s.Disabled {
		return false
	}
	if sets := s.sets.Load(); sets != nil {
		// Case-insensitive on host: the scope host is lowercased (hostOf), but a
		// switch value constructed directly may be mixed-case, and a kill switch
		// must fail closed rather than silently miss on case.
		host := strings.ToLower(scope.InboxHost)
		for disabled := range sets.hosts {
			if strings.ToLower(disabled) == host {
				return false
			}
		}
		if _, blocked := sets.communities[scope.CommunityAPID]; blocked {
			return false
		}
	}
	if _, blocked := s.DisabledActors[scope.ActorDID]; blocked {
		return false
	}
	return true
}

// SetDisabledSets replaces the disabled host and community sets. The next
// OutboundAllowed sees both new sets together. The caller must not change the
// maps afterwards.
func (s *ConfigSwitches) SetDisabledSets(hosts, communities map[string]struct{}) {
	s.sets.Store(&disabledSets{hosts: hosts, communities: communities})
}

// DryRun reports whether to translate + log without POSTing.
func (s *ConfigSwitches) DryRun() bool { return s.Dry }
