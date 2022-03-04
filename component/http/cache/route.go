package cache

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/beatlabs/patron/cache"
	"github.com/beatlabs/patron/log"
)

// RouteCache is the builder needed to build a cache for the corresponding route.
type RouteCache struct {
	// cache is the ttl cache implementation to be used.
	cache cache.TTLCache
	// age specifies the minimum and maximum amount for max-age and min-fresh Header values respectively
	// regarding the client cache-control requests in seconds.
	age age
}

// NewRouteCache creates a new cache implementation for an http route.
func NewRouteCache(ttlCache cache.TTLCache, age Age) (*RouteCache, []error) {
	errs := make([]error, 0)

	if ttlCache == nil {
		errs = append(errs, errors.New("route cache is nil"))
	}

	if age.Min > age.Max {
		errs = append(errs, errors.New("max age must always be greater than min age"))
	}

	if hasNoAgeConfig(age.Min.Milliseconds(), age.Max.Milliseconds()) {
		log.Warnf("route cache disabled because of empty Age property %v", age)
	}

	return &RouteCache{
		cache: ttlCache,
		age:   age.toAgeInSeconds(),
	}, errs
}

// Age defines the route cache life-time boundaries for cached objects.
type Age struct {
	// Min adds a minimum age threshold for the client controlled cache responses.
	// This will avoid cases where a single client with high request rate and no cache control headers might effectively disable the cache
	// This means that if this parameter is missing (e.g. is equal to '0' , the cache can effectively be made obsolete in the above scenario).
	Min time.Duration
	// Max adds a maximum age for the cache responses. Which effectively works as a time-to-live wrapper on top of the cache.
	Max time.Duration
	// The difference of maxAge-minAge sets automatically the max threshold for min-fresh requests
	// This will avoid cases where a single client with high request rate and no cache control headers might effectively disable the cache
	// This means that if this parameter is very high (e.g. greater than ttl , the cache can effectively be made obsolete in the above scenario).
}

func (a Age) toAgeInSeconds() age {
	return age{
		min: int64(a.Min / time.Second),
		max: int64(a.Max / time.Second),
	}
}

type age struct {
	min int64
	max int64
}

// responseReadWriter is a Response writer able to Read the Payload.
type responseReadWriter struct {
	buffer     *bytes.Buffer
	len        int
	header     http.Header
	statusCode int
}

// newResponseReadWriter creates a new responseReadWriter.
func newResponseReadWriter() *responseReadWriter {
	return &responseReadWriter{
		buffer: new(bytes.Buffer),
		header: make(http.Header),
	}
}

// Read reads the responsereadWriter Payload.
func (rw *responseReadWriter) Read(p []byte) (n int, err error) {
	return rw.buffer.Read(p)
}

// ReadAll returns the Response Payload Bytes.
func (rw *responseReadWriter) ReadAll() ([]byte, error) {
	if rw.len == 0 {
		// nothing has been written
		return []byte{}, nil
	}
	b := make([]byte, rw.len)
	_, err := rw.Read(b)
	return b, err
}

// Header returns the Header object.
func (rw *responseReadWriter) Header() http.Header {
	return rw.header
}

// Write writes the provied Bytes to the byte buffer.
func (rw *responseReadWriter) Write(p []byte) (int, error) {
	rw.len = len(p)
	return rw.buffer.Write(p)
}

// WriteHeader writes the Header status code.
func (rw *responseReadWriter) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
}

// Handler will wrap the handler func with the route cache abstraction.
func Handler(w http.ResponseWriter, r *http.Request, rc *RouteCache, httpHandler http.Handler) error {
	req := toCacheHandlerRequest(r)
	response, err := handler(httpExecutor(w, r, func(writer http.ResponseWriter, request *http.Request) {
		httpHandler.ServeHTTP(writer, request)
	}), rc)(req)
	if err != nil {
		return fmt.Errorf("could not handle request with the cache processor: %w", err)
	}
	for k, h := range response.Header {
		w.Header().Set(k, h[0])
	}
	if i, err := w.Write(response.Bytes); err != nil {
		return fmt.Errorf("could not Write cache processor result into Response %d: %w", i, err)
	}
	return nil
}

// httpExecutor is the function that will create a new response based on a HandlerFunc implementation
// this wrapper adapts the http handler signature to the cache layer abstraction.
func httpExecutor(_ http.ResponseWriter, request *http.Request, hnd http.HandlerFunc) executor {
	return func(now int64, key string) *response {
		var err error
		responseReadWriter := newResponseReadWriter()
		hnd(responseReadWriter, request)
		payload, err := responseReadWriter.ReadAll()
		rw := *responseReadWriter
		if err == nil {
			return &response{
				Response: handlerResponse{
					Bytes: payload,
					// cache also the headers generated by the handler
					Header: rw.Header(),
				},
				LastValid: now,
				Etag:      generateETag([]byte(key), time.Now().Nanosecond()),
			}
		}
		return &response{Err: err}
	}
}
