package operations

import "context"

type Operation struct {
	Name    string
	Execute func(context.Context) error
}
