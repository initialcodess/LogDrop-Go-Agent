package export

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync/atomic"
	"time"

	"github.com/initialcodess/LogDrop-Go-Agent/internal/core"
)

// Attempt is the outcome of one HTTP attempt: a status line, or a failure with or without hope of a later answer.
type Attempt struct {
	Status     int
	NoResponse bool
	AfterWrite bool
	Code       string
}

// Answered reports a status line.
func (a Attempt) Answered() bool { return a.Status > 0 }

// Transport sends one request body with a key.
type Transport interface {
	Send(body []byte, key string) Attempt
	Close()
}

// HTTPSender sends one OTLP/HTTP protobuf request, once: no retry, no redirect, no proxy, never traced itself (its
// client has its own transport, never the service's). Connections are pooled and kept alive between requests.
type HTTPSender struct {
	client    *http.Client
	url       string
	gzip      bool
	userAgent string
}

// NewHTTPSender sends to url. With caFile, that file's certificates alone are the trust anchors, like the Java and
// Node.js agents.
func NewHTTPSender(url, compression string, timeout time.Duration, caFile, userAgent string) (*HTTPSender, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("no certificate in the CA file")
		}
		tlsConfig.RootCAs = pool
	}
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:               nil,
		DialContext:         dialer.DialContext,
		TLSClientConfig:     tlsConfig,
		TLSHandshakeTimeout: timeout,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        2,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	client := &http.Client{
		Transport:     transport,
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &HTTPSender{client: client, url: url, gzip: compression == "gzip", userAgent: userAgent}, nil
}

// Send sends body once.
func (s *HTTPSender) Send(body []byte, key string) Attempt {
	var written atomic.Bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				written.Store(true)
			}
		},
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return Attempt{Code: "REQUEST"}
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	if s.gzip {
		request.Header.Set("Content-Encoding", "gzip")
	}
	request.Header.Set(core.IngestKeyHeader, key)
	request.Header.Set("User-Agent", s.userAgent)
	response, err := s.client.Do(request)
	if err != nil {
		return classify(err, written.Load())
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	_ = response.Body.Close()
	return Attempt{Status: response.StatusCode}
}

func classify(err error, written bool) Attempt {
	var timeout interface{ Timeout() bool }
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
		return Attempt{NoResponse: true, AfterWrite: written, Code: "LOGDROP_TIMEOUT"}
	}
	var certificate *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var record tls.RecordHeaderError
	if errors.As(err, &certificate) || errors.As(err, &unknownAuthority) || errors.As(err, &hostname) ||
		errors.As(err, &invalid) || errors.As(err, &record) {
		return Attempt{Code: "TLS"}
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return Attempt{NoResponse: true, AfterWrite: written, Code: "ENOTFOUND"}
	}
	var operation *net.OpError
	if errors.As(err, &operation) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return Attempt{NoResponse: true, AfterWrite: written, Code: "ECONNRESET"}
	}
	return Attempt{Code: "PROTOCOL"}
}

// Close closes idle connections.
func (s *HTTPSender) Close() { s.client.CloseIdleConnections() }

// String never shows the key.
func (s *HTTPSender) String() string { return "HTTPSender(redacted)" }
