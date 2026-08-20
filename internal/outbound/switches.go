package outbound

import "strings"

// ConfigSwitches is the config-backed Switches: the operator kill switches
// (decision 19) resolved from static configuration. It is the adapter main
// builds from config values and hands the Worker.
//
// A blocked delivery is PARKED by the worker (stays pending, resumes when the
// switch clears) — never poisoned or cancelled. The zero value (all fields
// empty) allows everything, so a deployment that sets no switches behaves
// exactly like AllowAll.
type ConfigSwitches struct {
	// Disabled is the global kill switch: when true, every delivery is blocked.
	Disabled bool
	// Dry, when true, makes the worker translate + log but POST nothing.
	Dry bool
	// DisabledHosts / Communities / Actors block a delivery whose inbox host,
	// community AP id, or actor DID is in the set. Nil sets block nothing.
	DisabledHosts       map[string]struct{}
	DisabledCommunities map[string]struct{}
	DisabledActors      map[string]struct{}
}

// OutboundAllowed reports whether a delivery in this scope may be sent: false
// when the global switch is engaged or the scope's host, community, or actor is
// in a disabled set.
func (s ConfigSwitches) OutboundAllowed(scope DeliveryScope) bool {
	if s.Disabled {
		return false
	}
	// Case-insensitive on host: the scope host is lowercased (hostOf), but a
	// switch value constructed directly may be mixed-case, and a kill switch
	// must fail closed rather than silently miss on case.
	host := strings.ToLower(scope.InboxHost)
	for disabled := range s.DisabledHosts {
		if strings.ToLower(disabled) == host {
			return false
		}
	}
	if _, blocked := s.DisabledCommunities[scope.CommunityAPID]; blocked {
		return false
	}
	if _, blocked := s.DisabledActors[scope.ActorDID]; blocked {
		return false
	}
	return true
}

// DryRun reports whether to translate + log without POSTing.
func (s ConfigSwitches) DryRun() bool { return s.Dry }
