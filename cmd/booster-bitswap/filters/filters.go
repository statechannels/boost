package filters

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// UpdateInterval is the default interval at which the public list is refected and updated
const UpdateInterval = 5 * time.Minute

// Fetcher is a function that fetches from a remote source
// The first return value indicates whether any update has occurred since the last fetch time
// The second return is a stream of data if an update has occurred
// The third is any error
type Fetcher func(lastFetchTime time.Time) (bool, io.ReadCloser, error)

const expectedListGrowth = 128

// FetcherForHTTPEndpoint makes an fetcher that reads from an HTTP endpoint
func FetcherForHTTPEndpoint(endpoint string) Fetcher {
	return func(ifModifiedSince time.Time) (bool, io.ReadCloser, error) {
		req, err := http.NewRequest("GET", endpoint, nil)
		if err != nil {
			return false, nil, err
		}
		// set the modification sync header, assuming we are not given time zero
		if !ifModifiedSince.IsZero() {
			req.Header.Set("If-Modified-Since", ifModifiedSince.Format(http.TimeFormat))
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return false, nil, err
		}
		if response.StatusCode == http.StatusNotModified {
			return false, nil, nil
		}
		if response.StatusCode < 200 && response.StatusCode > 299 {
			bodyText, _ := io.ReadAll(response.Body)
			return false, nil, fmt.Errorf("expected HTTP success code, got: %s, response body: %s", http.StatusText(response.StatusCode), string(bodyText))
		}
		return true, response.Body, nil
	}
}

type Handler interface {
	ParseUpdate(io.Reader) error
	// FulfillRequest returns true if a request should be fulfilled
	// error indicates an error in processing
	FulfillRequest(p peer.ID, c cid.Cid) (bool, error)
}

type filter struct {
	cacheFile   string
	lastUpdated time.Time
	fetcher     Fetcher
	handler     Handler
}

// update updates a filter from an endpoint
func (f *filter) update() error {
	fetchTime := time.Now()
	updated, stream, err := f.fetcher(f.lastUpdated)
	if err != nil {
		return fmt.Errorf("fetching endpoint: %w", err)

	}
	if !updated {
		return nil
	}
	defer stream.Close()
	// open the cache file
	cache, err := os.OpenFile(f.cacheFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("opening cache file: %w", err)
	}
	defer cache.Close()
	forkedStream := io.TeeReader(stream, cache)
	f.lastUpdated = fetchTime
	err = f.handler.ParseUpdate(forkedStream)
	if err != nil {
		return fmt.Errorf("parsing endpoint update: %w", err)
	}
	return nil
}

type MultiFilter struct {
	cfgDir     string
	filters    []*filter
	clock      clock.Clock
	onTimerSet func()
	ctx        context.Context
	cancel     context.CancelFunc
}

func newMultiFilter(cfgDir string, filters []*filter, clock clock.Clock, onTimerSet func()) *MultiFilter {
	return &MultiFilter{
		cfgDir:     cfgDir,
		filters:    filters,
		clock:      clock,
		onTimerSet: onTimerSet,
	}
}

func NewMultiFilter(cfgDir string, peerFilterEndpoint string) *MultiFilter {
	filters := []*filter{
		{
			cacheFile: filepath.Join(cfgDir, "denylist.json"),
			fetcher:   FetcherForHTTPEndpoint(BadBitsDenyList),
			handler:   NewBlockFilter(),
		},
	}
	if peerFilterEndpoint != "" {
		filters = append(filters, &filter{
			cacheFile: filepath.Join(cfgDir, "peerlist.json"),
			fetcher:   FetcherForHTTPEndpoint(peerFilterEndpoint),
			handler:   NewPeerFilter(),
		})
	}
	return newMultiFilter(cfgDir, filters, clock.New(), nil)
}

// Start initializes asynchronous updates to the filter configs
// It blocks to confirm at least one synchronous update of each filter
func (mf *MultiFilter) Start(ctx context.Context) error {
	mf.ctx, mf.cancel = context.WithCancel(ctx)
	var cachedCopies []bool
	for _, f := range mf.filters {
		// open the cache file if it eixsts
		cache, err := os.Open(f.cacheFile)
		// if the file does not exist, synchronously fetch the list
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("fetching badbits list: %w", err)
			}
			err = f.update()
			if err != nil {
				return err
			}
			cachedCopies = append(cachedCopies, false)
		} else {
			defer cache.Close()
			// otherwise, read the file and fetch the list asynchronously
			err = f.handler.ParseUpdate(cache)
			if err != nil {
				return err
			}
			cachedCopies = append(cachedCopies, true)
		}
	}
	go mf.run(cachedCopies)
	return nil
}

// Close shuts down asynchronous updating
func (mf *MultiFilter) Close() {
	mf.cancel()
}

// FulfillRequest returns true if a request should be fulfilled
// error indicates an error in processing
func (mf *MultiFilter) FulfillRequest(p peer.ID, c cid.Cid) (bool, error) {
	for _, f := range mf.filters {
		has, err := f.handler.FulfillRequest(p, c)
		if !has || err != nil {
			return has, err
		}
	}
	return true, nil
}

// run periodically updates the deny list asynchronously
func (mf *MultiFilter) run(cachedCopies []bool) {
	// if there was a cached copy, immediately asynchornously fetch an update
	for i, f := range mf.filters {
		if cachedCopies[i] {
			err := f.update()
			if err != nil {
				log.Error(err.Error())
			}
		}
	}
	updater := mf.clock.Ticker(UpdateInterval)
	// call the callback if set
	if mf.onTimerSet != nil {
		mf.onTimerSet()
	}
	for {
		select {
		case <-mf.ctx.Done():
			return
		case <-updater.C:
			// when timer expires, update deny list
			for _, f := range mf.filters {
				err := f.update()
				if err != nil {
					log.Error(err.Error())
				}
			}
			// call the callback if set
			if mf.onTimerSet != nil {
				mf.onTimerSet()
			}
		}
	}
}
