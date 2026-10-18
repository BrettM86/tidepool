package identity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"tidepool/internal/store"
)

// maxPLCLastOperationBody caps how much of a PLC /log/last response is read.
const maxPLCLastOperationBody = 64 << 10

const (
	maxPLCUpdateAttempts    = 5
	plcUpdateRetryBaseDelay = 200 * time.Millisecond
)

// validateLastOperation checks that last is a plc_operation carrying the
// fields an update copies forward, so a tombstone, legacy create or
// malformed head is never signed and submitted.
func validateLastOperation(last map[string]any) error {
	if last == nil {
		return errors.New("empty operation")
	}
	if operationType, _ := last["type"].(string); operationType != "plc_operation" {
		return fmt.Errorf("type %q is not plc_operation", fmt.Sprint(last["type"]))
	}
	rotationKeys, ok := last["rotationKeys"].([]any)
	if !ok {
		return errors.New("rotationKeys is missing or not an array")
	}
	for _, rotationKey := range rotationKeys {
		if _, ok := rotationKey.(string); !ok {
			return errors.New("rotationKeys contains a non-string entry")
		}
	}
	if _, ok := last["verificationMethods"].(map[string]any); !ok {
		return errors.New("verificationMethods is missing or not an object")
	}
	if _, ok := last["services"].(map[string]any); !ok {
		return errors.New("services is missing or not an object")
	}
	return nil
}

// HandleFor returns the handle a fresh mint of (actorType, preferredUsername,
// instance) would get now: a group's user label gets the "c-" prefix, and a
// person's label that would start with "c-" gets "u-".
func (m *Minter) HandleFor(ctx context.Context, actorType store.ActorType, preferredUsername, instance string) (string, error) {
	return m.availableHandle(ctx, actorType, preferredUsername, instance)
}

// HandleUpdateOutcome is how a PLC handle update ended.
type HandleUpdateOutcome int

const (
	HandleUpdateApplied HandleUpdateOutcome = iota + 1
	HandleUpdateAlreadyApplied
	HandleUpdateNotControlled
	HandleUpdateRejected
)

// HandleUpdateResult is the typed result of UpdateHandle.
type HandleUpdateResult struct {
	Outcome HandleUpdateOutcome
	Message string
}

// ErrPLCTransient marks a PLC failure that a later run may retry.
var ErrPLCTransient = errors.New("identity: PLC transient failure")

// errPLCHeadNotOperation marks a PLC head of another type, such as a
// tombstone or legacy create, which carries no rotationKeys to update under.
var errPLCHeadNotOperation = errors.New("identity: PLC head is not a plc_operation")

// UpdateHandle submits a signed PLC update operation setting newHandle.
// beforeSubmit, when not nil, runs right before each POST attempt; an error
// from it aborts the update without sending that POST.
func (m *Minter) UpdateHandle(ctx context.Context, did, newHandle string, beforeSubmit func(context.Context) error) (HandleUpdateResult, error) {
	first, err := m.readLastOperation(ctx, did)
	if errors.Is(err, errPLCHeadNotOperation) {
		return HandleUpdateResult{Outcome: HandleUpdateNotControlled}, nil
	}
	if err != nil {
		return HandleUpdateResult{}, err
	}
	rotationDIDKey, err := m.rotationDIDKey()
	if err != nil {
		return HandleUpdateResult{}, err
	}
	if !controlsOperation(first, rotationDIDKey) {
		return HandleUpdateResult{Outcome: HandleUpdateNotControlled}, nil
	}
	if operationShowsHandle(first, first, newHandle, rotationDIDKey) {
		return HandleUpdateResult{Outcome: HandleUpdateAlreadyApplied}, nil
	}
	op, err := m.buildHandleUpdate(first, newHandle)
	if err != nil {
		return HandleUpdateResult{}, err
	}
	second, err := m.readLastOperation(ctx, did)
	if errors.Is(err, errPLCHeadNotOperation) {
		return HandleUpdateResult{Outcome: HandleUpdateNotControlled}, nil
	}
	if err != nil {
		return HandleUpdateResult{}, err
	}
	if !controlsOperation(second, rotationDIDKey) {
		return HandleUpdateResult{Outcome: HandleUpdateNotControlled}, nil
	}
	if operationShowsHandle(second, op, newHandle, rotationDIDKey) {
		return HandleUpdateResult{Outcome: HandleUpdateAlreadyApplied}, nil
	}
	firstCID, err := operationCID(first)
	if err != nil {
		return HandleUpdateResult{}, fmt.Errorf("identity: CID for PLC first head of %s: %w", did, err)
	}
	secondCID, err := operationCID(second)
	if err != nil {
		return HandleUpdateResult{}, fmt.Errorf("identity: CID for PLC second head of %s: %w", did, err)
	}
	if firstCID != secondCID {
		op, err = m.buildHandleUpdate(second, newHandle)
		if err != nil {
			return HandleUpdateResult{}, err
		}
	}
	return m.postHandleUpdate(ctx, did, newHandle, rotationDIDKey, op, beforeSubmit)
}

func (m *Minter) rotationDIDKey() (string, error) {
	public, err := m.rotationKey.PublicKey()
	if err != nil {
		return "", fmt.Errorf("identity: derive PLC rotation public key: %w", err)
	}
	return public.DIDKey(), nil
}

func controlsOperation(head map[string]any, rotationDIDKey string) bool {
	rotationKeys, _ := head["rotationKeys"].([]any)
	for _, key := range rotationKeys {
		if key == rotationDIDKey {
			return true
		}
	}
	return false
}

func operationShowsHandle(head, expected map[string]any, handle, rotationDIDKey string) bool {
	if !controlsOperation(head, rotationDIDKey) ||
		!reflect.DeepEqual(head["verificationMethods"], expected["verificationMethods"]) ||
		!reflect.DeepEqual(head["services"], expected["services"]) {
		return false
	}
	aliases, ok := head["alsoKnownAs"].([]any)
	if !ok {
		return false
	}
	for _, alias := range aliases {
		if alias == "at://"+handle {
			return true
		}
	}
	return false
}

// readLastOperation fetches the DID's PLC head, backing off on 429 and 5xx.
// A head of another type wraps errPLCHeadNotOperation.
func (m *Minter) readLastOperation(ctx context.Context, did string) (map[string]any, error) {
	for attempt := 1; ; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			m.plcURL+"/"+url.PathEscape(did)+"/log/last", nil)
		if err != nil {
			return nil, fmt.Errorf("identity: build PLC last operation request for %s: %w", did, err)
		}
		request.Header.Set("User-Agent", m.userAgent)
		response, err := m.httpClient.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("identity: GET PLC last operation for %s: %w: %v", did, ErrPLCTransient, err)
		}
		status := response.StatusCode
		if status != http.StatusTooManyRequests && status < 500 {
			last, err := decodeLastOperation(did, response)
			_ = response.Body.Close()
			return last, err
		}
		_ = response.Body.Close()
		if attempt == maxPLCUpdateAttempts {
			return nil, fmt.Errorf("identity: GET PLC last operation for %s: %w: status %d after %d attempts",
				did, ErrPLCTransient, status, attempt)
		}
		delay := plcUpdateRetryBaseDelay * time.Duration(1<<(attempt-1))
		if err := m.waitForPLCRetry(ctx, delay); err != nil {
			return nil, fmt.Errorf("identity: wait to retry PLC last operation for %s: %w", did, err)
		}
	}
}

func decodeLastOperation(did string, response *http.Response) (map[string]any, error) {
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone {
		message, _ := io.ReadAll(io.LimitReader(response.Body, maxPLCErrorBody))
		return nil, fmt.Errorf("identity: GET PLC last operation for %s: status %d: %s",
			did, response.StatusCode, strings.TrimSpace(string(message)))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("identity: GET PLC last operation for %s: %w: status %d",
			did, ErrPLCTransient, response.StatusCode)
	}
	var last map[string]any
	if err := json.NewDecoder(io.LimitReader(response.Body, maxPLCLastOperationBody)).Decode(&last); err != nil {
		return nil, fmt.Errorf("identity: decode PLC last operation for %s: %w: %v", did, ErrPLCTransient, err)
	}
	if operationType, ok := last["type"].(string); ok && operationType != "plc_operation" {
		return nil, fmt.Errorf("identity: PLC last operation for %s: %w: type %q", did, errPLCHeadNotOperation, operationType)
	}
	if err := validateLastOperation(last); err != nil {
		return nil, fmt.Errorf("identity: PLC last operation for %s: %w: %v", did, ErrPLCTransient, err)
	}
	return last, nil
}

func operationCID(last map[string]any) (string, error) {
	encoded, err := atdata.MarshalCBOR(last)
	if err != nil {
		return "", fmt.Errorf("encode PLC last operation: %w", err)
	}
	previous, err := (cid.Prefix{Version: 1, Codec: cid.DagCBOR, MhType: multihash.SHA2_256, MhLength: 32}).Sum(encoded)
	if err != nil {
		return "", fmt.Errorf("CID for PLC last operation: %w", err)
	}
	return previous.String(), nil
}

func (m *Minter) buildHandleUpdate(last map[string]any, newHandle string) (map[string]any, error) {
	previous, err := operationCID(last)
	if err != nil {
		return nil, fmt.Errorf("identity: build PLC handle update: %w", err)
	}
	op := map[string]any{
		"type":                last["type"],
		"rotationKeys":        last["rotationKeys"],
		"verificationMethods": last["verificationMethods"],
		"services":            last["services"],
		"alsoKnownAs":         []any{"at://" + newHandle},
		"prev":                previous,
	}
	signed, err := m.signOperation(op)
	if err != nil {
		return nil, fmt.Errorf("identity: sign PLC handle update: %w", err)
	}
	return signed, nil
}

func (m *Minter) postHandleUpdate(ctx context.Context, did, newHandle, rotationDIDKey string, signed map[string]any, beforeSubmit func(context.Context) error) (HandleUpdateResult, error) {
	body, err := json.Marshal(signed)
	if err != nil {
		return HandleUpdateResult{}, fmt.Errorf("identity: marshal PLC handle update for %s: %w", did, err)
	}
	for attempt := 1; attempt <= maxPLCUpdateAttempts; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost,
			m.plcURL+"/"+url.PathEscape(did), bytes.NewReader(body))
		if err != nil {
			return HandleUpdateResult{}, fmt.Errorf("identity: build PLC handle update request for %s: %w", did, err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("User-Agent", m.userAgent)
		if beforeSubmit != nil {
			if err := beforeSubmit(ctx); err != nil {
				return HandleUpdateResult{}, fmt.Errorf("identity: before PLC handle update for %s: %w", did, err)
			}
		}
		response, err := m.httpClient.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return HandleUpdateResult{}, ctx.Err()
			}
			return m.reconcileHandleUpdate(ctx, did, newHandle, rotationDIDKey, signed, 0, err.Error())
		}
		message, _ := io.ReadAll(io.LimitReader(response.Body, maxPLCErrorBody))
		_ = response.Body.Close()
		status := response.StatusCode
		switch {
		case status >= 200 && status < 300:
			return HandleUpdateResult{Outcome: HandleUpdateApplied}, nil
		case status == http.StatusBadRequest:
			return m.reconcileHandleUpdate(ctx, did, newHandle, rotationDIDKey, signed, status, strings.TrimSpace(string(message)))
		case status == http.StatusTooManyRequests || status >= 500:
			if attempt == maxPLCUpdateAttempts {
				return HandleUpdateResult{}, fmt.Errorf("identity: POST PLC handle update for %s: %w: status %d after %d attempts", did, ErrPLCTransient, status, attempt)
			}
			delay := plcUpdateRetryBaseDelay * time.Duration(1<<(attempt-1))
			if err := m.waitForPLCRetry(ctx, delay); err != nil {
				return HandleUpdateResult{}, fmt.Errorf("identity: wait to retry PLC handle update for %s: %w", did, err)
			}
		default:
			return HandleUpdateResult{}, fmt.Errorf("identity: POST PLC handle update for %s: status %d: %s", did, status, strings.TrimSpace(string(message)))
		}
	}
	return HandleUpdateResult{}, fmt.Errorf("identity: POST PLC handle update for %s: %w: retries exhausted", did, ErrPLCTransient)
}

func (m *Minter) waitForPLCRetry(ctx context.Context, delay time.Duration) error {
	if m.plcRetryWait != nil {
		if err := m.plcRetryWait(ctx, delay); err != nil {
			return err
		}
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (m *Minter) reconcileHandleUpdate(ctx context.Context, did, newHandle, rotationDIDKey string, signed map[string]any, status int, message string) (HandleUpdateResult, error) {
	head, err := m.readLastOperation(ctx, did)
	if errors.Is(err, errPLCHeadNotOperation) {
		return HandleUpdateResult{Outcome: HandleUpdateNotControlled}, nil
	}
	if err != nil {
		return HandleUpdateResult{}, fmt.Errorf("identity: reconcile PLC handle update for %s: %w", did, err)
	}
	if operationShowsHandle(head, signed, newHandle, rotationDIDKey) {
		return HandleUpdateResult{Outcome: HandleUpdateAlreadyApplied}, nil
	}
	if status == http.StatusBadRequest {
		return HandleUpdateResult{Outcome: HandleUpdateRejected, Message: message}, nil
	}
	return HandleUpdateResult{}, fmt.Errorf("identity: POST PLC handle update for %s: %w: outcome unknown: %s", did, ErrPLCTransient, message)
}
