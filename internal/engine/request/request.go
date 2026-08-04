package request

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// BasicAuth represents basic HTTP authentication credentials.
type BasicAuth struct {
	Username string
	Password string
}

// RequestBuilder handles creation and configuration of http.Request objects.
type RequestBuilder struct {
	Method      string
	URL         string
	Headers     map[string]string
	Cookies     []*http.Cookie
	QueryParams map[string]string
	BasicAuth   *BasicAuth
	BearerToken string
	Body        []byte
	ContentType string
}

// NewRequestBuilder creates a default RequestBuilder.
func NewRequestBuilder(method, targetURL string) *RequestBuilder {
	return &RequestBuilder{
		Method:  method,
		URL:     targetURL,
		Headers: make(map[string]string),
	}
}

// Build compiles the parameters into an executable *http.Request.
func (b *RequestBuilder) Build(ctx context.Context) (*http.Request, error) {
	reqURL, err := url.Parse(b.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add query parameters
	if len(b.QueryParams) > 0 {
		q := reqURL.Query()
		for k, v := range b.QueryParams {
			q.Add(k, v)
		}
		reqURL.RawQuery = q.Encode()
	}

	var bodyReader bytes.Reader
	if len(b.Body) > 0 {
		bodyReader = *bytes.NewReader(b.Body)
	}

	req, err := http.NewRequestWithContext(ctx, b.Method, reqURL.String(), &bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set Headers
	for k, v := range b.Headers {
		req.Header.Set(k, v)
	}

	// Set Content-Type if specified
	if b.ContentType != "" {
		req.Header.Set("Content-Type", b.ContentType)
	}

	// Set Bearer Token
	if b.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+b.BearerToken)
	}

	// Set Basic Auth
	if b.BasicAuth != nil {
		req.SetBasicAuth(b.BasicAuth.Username, b.BasicAuth.Password)
	}

	// Add Cookies
	for _, cookie := range b.Cookies {
		req.AddCookie(cookie)
	}

	return req, nil
}
