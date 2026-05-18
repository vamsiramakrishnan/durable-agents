package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ProxyFaultKind — the eight v1 fault kinds the proxy honours.
type ProxyFaultKind string

const (
	FaultDelay          ProxyFaultKind = "delay"
	FaultInjectStatus   ProxyFaultKind = "inject_status"
	FaultTruncateStream ProxyFaultKind = "truncate_stream"
	FaultMangleJSON     ProxyFaultKind = "mangle_json"
	FaultInjectPrompt   ProxyFaultKind = "inject_prompt"
	FaultToolShadow     ProxyFaultKind = "tool_shadow"
	FaultSchemaDrift    ProxyFaultKind = "schema_drift"
	FaultDropConnection ProxyFaultKind = "drop_connection"
)

// ProxyFault — one declarative chaos rule the ChaosProxy applies.
type ProxyFault struct {
	Kind        ProxyFaultKind
	PathPrefix  string
	Probability float64

	Ms      int
	Jitter  float64
	Status  int
	Body    string
	AtEvent int

	JSONPath    string
	Replacement any
	Suffix      string
	ExtraTool   map[string]any
	DriftFn     func(payload any) any
}

// ── fault constructors ───────────────────────────────────────────────────

// PDelay — sleep ms ± jitter before forwarding.
func PDelay(pathPrefix string, ms int, probability float64) ProxyFault {
	return ProxyFault{Kind: FaultDelay, PathPrefix: pathPrefix, Ms: ms, Probability: probability}
}

// PInjectStatus — short-circuit with HTTP status + body.
func PInjectStatus(pathPrefix string, status int, body string, probability float64) ProxyFault {
	return ProxyFault{Kind: FaultInjectStatus, PathPrefix: pathPrefix, Status: status, Body: body, Probability: probability}
}

// PTruncateStream — cut SSE after `atEvent` events.
func PTruncateStream(pathPrefix string, atEvent int, probability float64) ProxyFault {
	return ProxyFault{Kind: FaultTruncateStream, PathPrefix: pathPrefix, AtEvent: atEvent, Probability: probability}
}

// PMangleJSON — replace a dotted JSON field on the response.
func PMangleJSON(pathPrefix, jsonPath string, replacement any, probability float64) ProxyFault {
	return ProxyFault{Kind: FaultMangleJSON, PathPrefix: pathPrefix, JSONPath: jsonPath, Replacement: replacement, Probability: probability}
}

// PInjectPrompt — append a suffix to top-level text/content fields.
func PInjectPrompt(pathPrefix, suffix string, probability float64) ProxyFault {
	return ProxyFault{Kind: FaultInjectPrompt, PathPrefix: pathPrefix, Suffix: suffix, Probability: probability}
}

// PToolShadow — inject an extra tool into an MCP tools/list response.
func PToolShadow(pathPrefix string, extraTool map[string]any, probability float64) ProxyFault {
	return ProxyFault{Kind: FaultToolShadow, PathPrefix: pathPrefix, ExtraTool: extraTool, Probability: probability}
}

// PSchemaDrift — apply an arbitrary transform (escape hatch).
func PSchemaDrift(pathPrefix string, driftFn func(any) any, probability float64) ProxyFault {
	return ProxyFault{Kind: FaultSchemaDrift, PathPrefix: pathPrefix, DriftFn: driftFn, Probability: probability}
}

// PDropConnection — close the socket mid-response.
func PDropConnection(pathPrefix string, probability float64) ProxyFault {
	return ProxyFault{Kind: FaultDropConnection, PathPrefix: pathPrefix, Probability: probability}
}

// ── helpers ──────────────────────────────────────────────────────────────

func setJSONAt(obj any, path string, value any) any {
	if path == "" {
		return value
	}
	parts := strings.Split(path, ".")
	cur := obj
	for i := 0; i < len(parts)-1; i++ {
		p := parts[i]
		if arr, ok := cur.([]any); ok {
			n, err := strconv.Atoi(p)
			if err != nil || n < 0 || n >= len(arr) {
				return obj
			}
			cur = arr[n]
		} else if m, ok := cur.(map[string]any); ok {
			cur = m[p]
		} else {
			return obj
		}
	}
	last := parts[len(parts)-1]
	if arr, ok := cur.([]any); ok {
		n, err := strconv.Atoi(last)
		if err == nil && n >= 0 && n < len(arr) {
			arr[n] = value
		}
	} else if m, ok := cur.(map[string]any); ok {
		m[last] = value
	}
	return obj
}

func injectPromptInto(obj any, suffix string) any {
	switch v := obj.(type) {
	case map[string]any:
		for _, k := range []string{"text", "content", "output_text"} {
			if s, ok := v[k].(string); ok {
				v[k] = s + suffix
			}
		}
		for _, vv := range v {
			injectPromptInto(vv, suffix)
		}
	case []any:
		for _, vv := range v {
			injectPromptInto(vv, suffix)
		}
	}
	return obj
}

func shadowTools(obj any, extra map[string]any) any {
	switch v := obj.(type) {
	case map[string]any:
		for k, vv := range v {
			if k == "tools" {
				if arr, ok := vv.([]any); ok {
					cp := map[string]any{}
					for ek, ev := range extra {
						cp[ek] = ev
					}
					v[k] = append(arr, cp)
				}
			} else {
				shadowTools(vv, extra)
			}
		}
	case []any:
		for _, vv := range v {
			shadowTools(vv, extra)
		}
	}
	return obj
}

var dropRequestHeaders = map[string]struct{}{
	"host": {}, "content-length": {}, "connection": {}, "keep-alive": {},
	"proxy-authenticate": {}, "proxy-authorization": {}, "te": {},
	"trailers": {}, "transfer-encoding": {}, "upgrade": {},
}

// ── ChaosProxy ───────────────────────────────────────────────────────────

// ChaosProxy — a Go HTTP forward-proxy with declarative chaos rules.
// Streams SSE chunk-by-chunk. The proxy is plaintext-to-agent; it
// upgrades to TLS on the way out if the upstream is `https://`.
type ChaosProxy struct {
	Upstream  string
	Faults    []ProxyFault
	Timeout   time.Duration
	rng       *rand.Rand
	server    *http.Server
	listener  net.Listener
	url       string
	mu        sync.Mutex
	faultHits map[string]int
}

// ChaosProxyOpts — knobs for the proxy.
type ChaosProxyOpts struct {
	Rng     *rand.Rand
	Timeout time.Duration
}

// NewChaosProxy — construct (do not Start).
func NewChaosProxy(upstream string, faults []ProxyFault, opts ChaosProxyOpts) *ChaosProxy {
	p := &ChaosProxy{
		Upstream:  strings.TrimRight(upstream, "/"),
		Faults:    faults,
		Timeout:   opts.Timeout,
		rng:       opts.Rng,
		faultHits: map[string]int{},
	}
	if p.Timeout == 0 {
		p.Timeout = 60 * time.Second
	}
	if p.rng == nil {
		p.rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return p
}

// URL — the proxy's listening URL, populated by Start.
func (p *ChaosProxy) URL() string { return p.url }

// FaultHits — snapshot of per-(kind, pathPrefix) fire counts.
func (p *ChaosProxy) FaultHits() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.faultHits))
	for k, v := range p.faultHits {
		out[k] = v
	}
	return out
}

// Start — bind on `host:port`. Port 0 picks a free port.
func (p *ChaosProxy) Start(host string, port int) error {
	if host == "" {
		host = "127.0.0.1"
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	p.listener = ln
	p.url = fmt.Sprintf("http://%s", ln.Addr().String())
	p.server = &http.Server{Handler: http.HandlerFunc(p.handle)}
	go func() { _ = p.server.Serve(ln) }()
	return nil
}

// Stop — graceful shutdown.
func (p *ChaosProxy) Stop(ctx context.Context) error {
	if p.server == nil {
		return nil
	}
	return p.server.Shutdown(ctx)
}

func (p *ChaosProxy) matching(path string, kind ProxyFaultKind) []ProxyFault {
	var out []ProxyFault
	for _, f := range p.Faults {
		if f.Kind != kind {
			continue
		}
		if f.PathPrefix != "" && !strings.HasPrefix(path, f.PathPrefix) {
			continue
		}
		if f.Probability >= 1.0 || p.rng.Float64() < f.Probability {
			out = append(out, f)
			key := fmt.Sprintf("%s:%s", kind, f.PathPrefix)
			p.mu.Lock()
			p.faultHits[key]++
			p.mu.Unlock()
		}
	}
	return out
}

func (p *ChaosProxy) handle(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}

	// PRE-FORWARD: delay
	for _, f := range p.matching(r.URL.Path, FaultDelay) {
		ms := float64(f.Ms)
		if f.Jitter > 0 {
			ms = ms * (1.0 + (p.rng.Float64()*2-1)*f.Jitter)
			if ms < 0 {
				ms = 0
			}
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

	// PRE-FORWARD: inject_status
	if injected := p.matching(r.URL.Path, FaultInjectStatus); len(injected) > 0 {
		f := injected[0]
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Tape-Chaos", "inject_status")
		w.Header().Set("Content-Length", strconv.Itoa(len(f.Body)))
		w.WriteHeader(f.Status)
		_, _ = io.WriteString(w, f.Body)
		return
	}

	// Forward
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	target, err := url.Parse(p.Upstream + path)
	if err != nil {
		http.Error(w, "bad upstream URL", http.StatusInternalServerError)
		return
	}
	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		http.Error(w, "upstream request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for k, vs := range r.Header {
		if _, drop := dropRequestHeaders[strings.ToLower(k)]; drop {
			continue
		}
		for _, v := range vs {
			upstreamReq.Header.Add(k, v)
		}
	}
	client := &http.Client{Timeout: p.Timeout}
	resp, err := client.Do(upstreamReq)
	if err != nil {
		w.Header().Set("X-Tape-Chaos", "upstream-unreachable")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "upstream unreachable: "+err.Error())
		return
	}
	defer resp.Body.Close()

	ctype := resp.Header.Get("Content-Type")
	switch {
	case strings.HasPrefix(ctype, "text/event-stream"):
		p.replyStream(r.URL.Path, resp, w)
	case strings.HasPrefix(ctype, "application/json"):
		p.replyJSON(r.URL.Path, resp, w)
	default:
		p.replyPassthrough(r.URL.Path, resp, w)
	}
}

func (p *ChaosProxy) writeHeaders(resp *http.Response, w http.ResponseWriter, overrideLen int, extra map[string]string) {
	for k, vs := range resp.Header {
		lk := strings.ToLower(k)
		if lk == "transfer-encoding" || lk == "content-encoding" ||
			lk == "connection" || lk == "keep-alive" {
			continue
		}
		if overrideLen >= 0 && lk == "content-length" {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	if overrideLen >= 0 {
		w.Header().Set("Content-Length", strconv.Itoa(overrideLen))
	}
	for k, v := range extra {
		w.Header().Set(k, v)
	}
	w.WriteHeader(resp.StatusCode)
}

func (p *ChaosProxy) replyPassthrough(path string, resp *http.Response, w http.ResponseWriter) {
	data, _ := io.ReadAll(resp.Body)
	drops := p.matching(path, FaultDropConnection)
	p.writeHeaders(resp, w, len(data), map[string]string{"X-Tape-Chaos": "passthrough"})
	if len(drops) > 0 {
		return // headers sent, body intentionally omitted
	}
	_, _ = w.Write(data)
}

func (p *ChaosProxy) replyJSON(path string, resp *http.Response, w http.ResponseWriter) {
	data, _ := io.ReadAll(resp.Body)
	var payload any
	if err := json.Unmarshal(data, &payload); err != nil {
		p.writeHeaders(resp, w, len(data), nil)
		_, _ = w.Write(data)
		return
	}
	var applied []string
	for _, f := range p.matching(path, FaultMangleJSON) {
		payload = setJSONAt(payload, f.JSONPath, f.Replacement)
		applied = append(applied, "mangle_json")
	}
	for _, f := range p.matching(path, FaultInjectPrompt) {
		payload = injectPromptInto(payload, f.Suffix)
		applied = append(applied, "inject_prompt")
	}
	for _, f := range p.matching(path, FaultToolShadow) {
		if f.ExtraTool != nil {
			payload = shadowTools(payload, f.ExtraTool)
			applied = append(applied, "tool_shadow")
		}
	}
	for _, f := range p.matching(path, FaultSchemaDrift) {
		if f.DriftFn != nil {
			func() {
				defer func() { _ = recover() }()
				payload = f.DriftFn(payload)
			}()
			applied = append(applied, "schema_drift")
		}
	}
	drops := p.matching(path, FaultDropConnection)
	newBody, _ := json.Marshal(payload)
	tag := strings.Join(applied, ",")
	if tag == "" {
		tag = "json"
	}
	p.writeHeaders(resp, w, len(newBody), map[string]string{"X-Tape-Chaos": tag})
	if len(drops) > 0 {
		return
	}
	_, _ = w.Write(newBody)
}

func (p *ChaosProxy) replyStream(path string, resp *http.Response, w http.ResponseWriter) {
	truncates := p.matching(path, FaultTruncateStream)
	cutAt := 0
	if len(truncates) > 0 {
		cutAt = truncates[0].AtEvent
		for _, t := range truncates[1:] {
			if t.AtEvent < cutAt {
				cutAt = t.AtEvent
			}
		}
	}
	drops := p.matching(path, FaultDropConnection)
	tag := "sse"
	if len(truncates) > 0 {
		tag = "truncate_stream"
	} else if len(drops) > 0 {
		tag = "drop_connection"
	}
	p.writeHeaders(resp, w, -1, map[string]string{"X-Tape-Chaos": tag})

	flusher, _ := w.(http.Flusher)
	buf := bytes.Buffer{}
	chunk := make([]byte, 2048)
	eventCount := 0
	for {
		n, err := resp.Body.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
			for {
				idx := bytes.Index(buf.Bytes(), []byte("\n\n"))
				if idx < 0 {
					break
				}
				evt := buf.Next(idx + 2)
				eventCount++
				if _, werr := w.Write(evt); werr != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
				if cutAt > 0 && eventCount >= cutAt {
					return
				}
			}
			if len(drops) > 0 && eventCount >= 1 {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// ── convenience constructors ─────────────────────────────────────────────

// ModelProxy — a ChaosProxy tuned for an LLM provider's base URL.
func ModelProxy(upstream string, faults []ProxyFault, opts ChaosProxyOpts) *ChaosProxy {
	return NewChaosProxy(upstream, faults, opts)
}

// MCPProxy — a ChaosProxy tuned for an MCP server's HTTP/SSE endpoint.
func MCPProxy(upstream string, faults []ProxyFault, opts ChaosProxyOpts) *ChaosProxy {
	return NewChaosProxy(upstream, faults, opts)
}
