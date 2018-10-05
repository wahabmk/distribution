package storage

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

// TODO(stevvooe): This should configurable in the future.
const blobCacheControlMaxAge = 365 * 24 * time.Hour

const (
	DownstreamKey = "auth.downstream"
)

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver   driver.StorageDriver
	statter  distribution.BlobStatter
	pathFn   func(dgst digest.Digest) (string, error)
	redirect bool // allows disabling URLFor redirects
}

func (bs *blobServer) shouldUseRedirect(ctx context.Context, method string) bool {
	// If redirect for blobServer is set, definitely use redirect
	if bs.redirect {
		return true
	}

	// Content Caches are configured in dtr as a "downstream" host in the context as part of Garant authorization.
	// The Content Cache should still be used even if redirect is not enabled.
	_, ok := ctx.Value(DownstreamKey).(string)
	// If downstream host is not configured, return false
	if !ok {
		return false
	}

	// We only need to redirect to content cache on a GET request
	if method != http.MethodGet {
		return false
	}

	// If we get to this point we have a "downstream" key set in the context of a GET request
	return true
}

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	path, err := bs.pathFn(desc.Digest)
	if err != nil {
		return err
	}

	if bs.shouldUseRedirect(ctx, r.Method) {
		redirectURL, err := bs.driver.URLFor(ctx, path, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return err
		}
	}

	br, err := newFileReader(ctx, bs.driver, path, desc.Size)
	if err != nil {
		return err
	}
	defer br.Close()

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
	}

	http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)
	return nil
}
