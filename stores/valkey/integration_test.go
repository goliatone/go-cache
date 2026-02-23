package valkey

import (
	"os"
	"testing"

	gocache "github.com/goliatone/go-cache"
	"github.com/goliatone/go-cache/internal/conformancetest"
	vk "github.com/valkey-io/valkey-go"
)

func TestValkeyIntegrationConformance(t *testing.T) {
	address := os.Getenv("VALKEY_ADDR")
	if address == "" {
		t.Skip("VALKEY_ADDR not set; skipping valkey integration test")
	}

	factory := conformancetest.Factory[string, string]{
		Name: "valkey-integration",
		New: func(t *testing.T, options conformancetest.Options[string, string]) gocache.Cache[string, string] {
			t.Helper()
			opts := []Option[string, string]{
				WithAddress[string, string](address),
				WithNamespace[string, string]("itest"),
				WithClientOption[string, string](vk.ClientOption{
					ForceSingleClient: true,
					DisableCache:      true,
				}),
			}
			if options.Codec != nil {
				opts = append(opts, WithCodec[string, string](options.Codec))
			}
			if options.KeyEncoder != nil {
				opts = append(opts, WithKeyEncoder[string, string](options.KeyEncoder))
			}
			if options.Observer != nil {
				opts = append(opts, WithObserver[string, string](options.Observer))
			}

			store, err := NewStore[string, string](opts...)
			if err != nil {
				t.Fatalf("new store failed: %v", err)
			}
			t.Cleanup(func() {
				_ = store.Close()
			})
			return store
		},
	}

	conformancetest.RunStringCacheContractTests(t, factory)
}
