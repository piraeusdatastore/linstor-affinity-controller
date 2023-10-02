package util

import (
	"context"
	"sync"
	"time"

	lclient "github.com/LINBIT/golinstor/client"
)

type linstorResourceCacheProvider struct {
	lclient.ResourceProvider
	timeout     time.Duration
	mu          sync.Mutex
	ressUpdated time.Time
	ressCache   []lclient.ResourceWithVolumes
}

func NewResourceCache(provider lclient.ResourceProvider, timeout time.Duration) lclient.ResourceProvider {
	return &linstorResourceCacheProvider{
		ResourceProvider: provider,
		timeout:          timeout,
	}
}

func (l *linstorResourceCacheProvider) GetAll(ctx context.Context, resName string, opts ...*lclient.ListOpts) ([]lclient.Resource, error) {
	now := time.Now()

	if l.ressUpdated.Add(l.timeout).After(now) {
		return filter(l.ressCache, resName)
	}

	ress, err := l.ResourceProvider.GetResourceView(ctx)
	if err != nil {
		return nil, err
	}

	l.ressCache = ress
	l.ressUpdated = now

	return filter(l.ressCache, resName)
}

func filter(ress []lclient.ResourceWithVolumes, resName string) ([]lclient.Resource, error) {
	var result []lclient.Resource
	var err error

	for i := range ress {
		if ress[i].Name == resName {
			result = append(result, ress[i].Resource)
		}
	}

	if len(result) == 0 {
		err = lclient.NotFoundError
	}

	return result, err
}
