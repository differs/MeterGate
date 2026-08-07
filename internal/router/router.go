package router

import (
	"fmt"
	"os"
	"sync/atomic"

	"gopkg.in/yaml.v3"
)

// Snapshot is the immutable routing table the engine selects from.
// The gateway holds a single atomic pointer; control-plane updates swap
// in a new snapshot without touching the hot path (M3: config file reload;
// later: control-plane push).
type Snapshot struct {
	Version int64
	Models  map[string]*ModelRoute
}

// Router owns the current snapshot plus per-channel runtime state
// (health tracker + breaker) and serves selections.
type Router struct {
	Engine
	snap atomic.Pointer[Snapshot]

	health   *HealthTracker
	breakers map[string]*Breaker
	auto     AutoRouter // optional "auto" model picker
}

// NewRouter builds a router from a snapshot, wiring health trackers and
// breakers for every channel.
func NewRouter(snap *Snapshot) *Router { return NewRouterWithAuto(snap, nil) }

// NewRouterWithAuto wires the router with an optional auto router.
func NewRouterWithAuto(snap *Snapshot, auto AutoRouter) *Router {
	r := &Router{
		Engine:   *NewEngine(),
		health:   NewHealthTracker(0, nil),
		breakers: map[string]*Breaker{},
	}
	for _, route := range snap.Models {
		for _, c := range route.Channels {
			if _, ok := r.breakers[c.Channel.ID]; !ok {
				r.breakers[c.Channel.ID] = NewBreaker(nil)
			}
			// Prewarm health to "healthy": no data yet must not lock a
			// channel out of selection.
			r.health.healthy[c.Channel.ID] = true
		}
	}
	r.snap.Store(snap)
	return r
}

// Auto returns the auto router (nil when not configured).
func (r *Router) Auto() AutoRouter { return r.auto }

// Route returns the decision for a model from the current snapshot.
// Health/breaker state is folded into the channel specs on the fly —
// the cost is a couple of atomic reads per channel, still no locks.
func (r *Router) Route(model string) (Decision, error) {
	snap := r.snap.Load()
	route, ok := snap.Models[model]
	if !ok {
		return Decision{}, ErrModelNotFound
	}

	// Copy specs with live health bits (cheap, bounded channel count).
	// BreakerOpen uses the read-only IsOpen check; the dispatching layer
	// consumes half-open probes via Allow() when it actually forwards.
	specs := make([]ChannelSpec, len(route.Channels))
	for i, c := range route.Channels {
		specs[i] = ChannelSpec{
			Channel:     c.Channel,
			Healthy:     r.health.Healthy(c.Channel.ID),
			BreakerOpen: r.breakers[c.Channel.ID].IsOpen(),
		}
	}
	copyRoute := &ModelRoute{Model: route.Model, Channels: specs}
	return r.Engine.Select(copyRoute), nil
}

// RecordOutcome feeds a channel outcome back into health + breaker.
func (r *Router) RecordOutcome(channelID string, ok bool) {
	r.health.Record(channelID, ok)
	if b, exists := r.breakers[channelID]; exists {
		b.Record(ok)
	}
}

// ChannelHealth exposes current health for observability.
func (r *Router) ChannelHealth(channelID string) (healthy bool, breakerState string) {
	healthy = r.health.Healthy(channelID)
	if b, ok := r.breakers[channelID]; ok {
		return healthy, b.State()
	}
	return healthy, "unknown"
}

// --- config loading -------------------------------------------------------

// Config is the routing configuration file shape.
type Config struct {
	Channels []ChannelConfig `yaml:"channels"`
	Models   []ModelConfig   `yaml:"models"`
	// AutoRouter is optional: enables the "auto" model (local scoring).
	AutoRouter *AutoRouterConfig `yaml:"auto_router"`
}

// AutoRouterConfig configures the local auto model picker.
type AutoRouterConfig struct {
	// CostQuality 0-10: 0 = always most capable, 10 = always cheapest.
	CostQuality int `yaml:"cost_quality"`
	// Models is the candidate pool (model slugs that must exist in `models`).
	Models []string `yaml:"models"`
}

// ChannelConfig is one upstream channel in the config file.
type ChannelConfig struct {
	ID          string `yaml:"id"`
	Name        string `yaml:"name"`
	BaseURL     string `yaml:"base_url"`
	KeyEnv      string `yaml:"key_env"`       // env var holding the API key
	InputPer1M  int64  `yaml:"input_per_1m"`  // micros per 1M tokens
	OutputPer1M int64  `yaml:"output_per_1m"` // micros per 1M tokens
	Weight      int64  `yaml:"weight"`
}

// ModelConfig maps a model slug to a list of channel IDs.
type ModelConfig struct {
	Model    string   `yaml:"model"`
	Channels []string `yaml:"channels"`
}

// LoadConfig reads and validates the routing config file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if len(cfg.Channels) == 0 {
		return nil, fmt.Errorf("config: no channels defined")
	}
	if len(cfg.Models) == 0 {
		return nil, fmt.Errorf("config: no models defined")
	}
	return &cfg, nil
}

// BuildSnapshot turns a Config into an immutable routing snapshot,
// resolving key env vars and model→channel mappings.
func BuildSnapshot(cfg *Config) (*Snapshot, error) {
	byID := make(map[string]*Channel, len(cfg.Channels))
	for _, cc := range cfg.Channels {
		key := os.Getenv(cc.KeyEnv)
		if key == "" {
			return nil, fmt.Errorf("config: channel %q: env %q not set", cc.ID, cc.KeyEnv)
		}
		if cc.BaseURL == "" {
			return nil, fmt.Errorf("config: channel %q: base_url required", cc.ID)
		}
		byID[cc.ID] = &Channel{
			ID:          cc.ID,
			Name:        cc.Name,
			BaseURL:     cc.BaseURL,
			Key:         key,
			InputPer1M:  cc.InputPer1M,
			OutputPer1M: cc.OutputPer1M,
			Weight:      cc.Weight,
		}
	}

	models := make(map[string]*ModelRoute, len(cfg.Models))
	for _, mc := range cfg.Models {
		if len(mc.Channels) == 0 {
			return nil, fmt.Errorf("config: model %q has no channels", mc.Model)
		}
		route := &ModelRoute{Model: mc.Model, Channels: make([]ChannelSpec, 0, len(mc.Channels))}
		for _, cid := range mc.Channels {
			ch, ok := byID[cid]
			if !ok {
				return nil, fmt.Errorf("config: model %q references unknown channel %q", mc.Model, cid)
			}
			route.Channels = append(route.Channels, ChannelSpec{Channel: ch, Healthy: true})
		}
		models[mc.Model] = route
	}

	return &Snapshot{Version: 1, Models: models}, nil
}

// BuildAutoRouter builds the optional auto router from config.
func BuildAutoRouter(cfg *Config, snap *Snapshot) (AutoRouter, error) {
	if cfg.AutoRouter == nil || len(cfg.AutoRouter.Models) == 0 {
		return nil, nil
	}
	pool := make([]AutoCandidate, 0, len(cfg.AutoRouter.Models))
	for _, m := range cfg.AutoRouter.Models {
		route, ok := snap.Models[m]
		if !ok {
			return nil, fmt.Errorf("config: auto_router model %q not defined in models", m)
		}
		// pool cost = cheapest channel cost of that model
		var inp, out int64 = 1<<62 - 1, 1<<62 - 1
		for _, c := range route.Channels {
			if c.Channel.OutputPer1M < out {
				out = c.Channel.OutputPer1M
			}
			if c.Channel.InputPer1M < inp {
				inp = c.Channel.InputPer1M
			}
		}
		pool = append(pool, AutoCandidate{Model: m, InputPer1M: inp, OutputPer1M: out})
	}
	return NewLocalAutoRouter(pool, cfg.AutoRouter.CostQuality), nil
}
