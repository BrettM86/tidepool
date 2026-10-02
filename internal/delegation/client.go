// Package delegation reconciles bridged instance labels with Cloudflare NS
// records for the bridge's DNS zone.
package delegation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// CloudflareAPIBaseURL is the production Cloudflare v4 API endpoint.
const CloudflareAPIBaseURL = "https://api.cloudflare.com/client/v4"

const maximumResponseBytes = 1 << 20

// maximumListingPages bounds the NS listing at 10,000 records, well above the
// zone's record quota.
const maximumListingPages = 100

// CloudflareOptions configures the Cloudflare DNS client. HTTPClient must be
// supplied by the caller with the appropriate outbound-address guard.
type CloudflareOptions struct {
	BaseURL    string
	Token      string
	ZoneID     string
	HTTPClient *http.Client
}

// CloudflareClient lists and creates DNS records in a Cloudflare zone.
type CloudflareClient struct {
	baseURL    string
	token      string
	zoneID     string
	httpClient *http.Client
}

// NewCloudflareClient validates options and creates a Cloudflare DNS client.
func NewCloudflareClient(options CloudflareOptions) (*CloudflareClient, error) {
	if strings.TrimSpace(options.BaseURL) == "" || strings.TrimSpace(options.Token) == "" || strings.TrimSpace(options.ZoneID) == "" || options.HTTPClient == nil {
		return nil, fmt.Errorf("Cloudflare base URL, token, zone ID and HTTP client are required")
	}
	return &CloudflareClient{
		baseURL: strings.TrimRight(options.BaseURL, "/"),
		token:   options.Token, zoneID: options.ZoneID, httpClient: options.HTTPClient,
	}, nil
}

type dnsRecord struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Content string `json:"content"`
}

type apiResponse struct {
	Success bool `json:"success"`
	Errors  []struct {
		Message string `json:"message"`
	} `json:"errors"`
	Result     json.RawMessage `json:"result"`
	ResultInfo *struct {
		TotalPages int `json:"total_pages"`
	} `json:"result_info"`
}

func (c *CloudflareClient) recordsURL() string {
	return c.baseURL + "/zones/" + url.PathEscape(c.zoneID) + "/dns_records"
}

func (c *CloudflareClient) request(ctx context.Context, method, endpoint string, body []byte) (apiResponse, error) {
	var response apiResponse
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return response, fmt.Errorf("build Cloudflare DNS request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	result, err := c.httpClient.Do(request)
	if err != nil {
		return response, fmt.Errorf("send Cloudflare DNS request: %w", err)
	}
	defer result.Body.Close()
	data, err := io.ReadAll(io.LimitReader(result.Body, maximumResponseBytes+1))
	if err != nil {
		return response, fmt.Errorf("read Cloudflare DNS response: %w", err)
	}
	if len(data) > maximumResponseBytes {
		return response, fmt.Errorf("Cloudflare DNS response exceeds %d bytes", maximumResponseBytes)
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return response, fmt.Errorf("decode Cloudflare DNS response (HTTP %d): %w", result.StatusCode, err)
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 || !response.Success {
		messages := make([]string, 0, len(response.Errors))
		for _, apiError := range response.Errors {
			if apiError.Message != "" {
				messages = append(messages, apiError.Message)
			}
		}
		if len(messages) == 0 {
			return response, fmt.Errorf("Cloudflare DNS request failed (HTTP %d)", result.StatusCode)
		}
		return response, fmt.Errorf("Cloudflare DNS request failed (HTTP %d): %s", result.StatusCode, strings.Join(messages, "; "))
	}
	return response, nil
}

func (c *CloudflareClient) listRecords(ctx context.Context) ([]dnsRecord, error) {
	var records []dnsRecord
	for page := 1; ; page++ {
		query := url.Values{"type": {"NS"}, "per_page": {"100"}, "page": {fmt.Sprint(page)}}
		response, err := c.request(ctx, http.MethodGet, c.recordsURL()+"?"+query.Encode(), nil)
		if err != nil {
			return nil, fmt.Errorf("list Cloudflare NS records on page %d: %w", page, err)
		}
		var pageRecords []dnsRecord
		if err := json.Unmarshal(response.Result, &pageRecords); err != nil {
			return nil, fmt.Errorf("decode Cloudflare NS records on page %d: %w", page, err)
		}
		if response.ResultInfo == nil {
			return nil, fmt.Errorf("list Cloudflare NS records on page %d: response has no result_info", page)
		}
		totalPages := response.ResultInfo.TotalPages
		if totalPages < 1 && len(pageRecords) > 0 {
			return nil, fmt.Errorf("list Cloudflare NS records on page %d: result_info.total_pages is %d but the page returned %d records", page, totalPages, len(pageRecords))
		}
		records = append(records, pageRecords...)
		if page >= totalPages {
			return records, nil
		}
		if page >= maximumListingPages {
			return nil, fmt.Errorf("list Cloudflare NS records: result_info.total_pages is %d, above the %d-page limit", totalPages, maximumListingPages)
		}
	}
}

func (c *CloudflareClient) createRecord(ctx context.Context, name, content string) error {
	body, err := json.Marshal(struct {
		Type    string `json:"type"`
		Name    string `json:"name"`
		Content string `json:"content"`
		TTL     int    `json:"ttl"`
	}{Type: "NS", Name: name, Content: content, TTL: 3600})
	if err != nil {
		return fmt.Errorf("encode Cloudflare NS record: %w", err)
	}
	if _, err := c.request(ctx, http.MethodPost, c.recordsURL(), body); err != nil {
		return fmt.Errorf("create Cloudflare NS record %s: %w", name, err)
	}
	return nil
}
