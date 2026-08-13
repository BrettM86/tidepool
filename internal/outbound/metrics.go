package outbound

import "expvar"

// Delivery outcome counters, published under the tidepool_ prefix so they
// surface at /admin/metrics (ingest.Admin's scopedMetrics filters to that
// prefix). The worker bumps exactly one per terminal outcome, plus parked for
// each kill-switch/dry-run/causal-wait deferral.
var (
	metricDelivered = expvar.NewInt("tidepool_outbound_delivered")
	metricPoisoned  = expvar.NewInt("tidepool_outbound_poisoned")
	metricCancelled = expvar.NewInt("tidepool_outbound_cancelled")
	metricParked    = expvar.NewInt("tidepool_outbound_parked")
)
