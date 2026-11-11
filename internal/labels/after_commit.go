package labels

import (
	"context"
	stderrors "errors"
)

// afterCommitKey carries the hooks of the label transaction an applier runs
// in.
type afterCommitKey struct{}

type afterCommitHooks struct{ funcs []func(context.Context) }

// errNoLabelTransaction refuses AfterCommit outside a store's label
// transaction.
var errNoLabelTransaction = stderrors.New("labels: AfterCommit called outside a label transaction")

// AfterCommit registers f to run once the label transaction the applier was
// called in has committed, so a network send an applier owes (such as a
// community's Undo{Follow}) never runs while the transaction holds its locks,
// and never runs for a transaction that rolls back. Hooks run in registration
// order on the committing goroutine, with the store call's context. It returns
// an error when ctx is not an applier's context from this package's store.
func AfterCommit(ctx context.Context, f func(context.Context)) error {
	hooks, ok := ctx.Value(afterCommitKey{}).(*afterCommitHooks)
	if !ok {
		return errNoLabelTransaction
	}
	hooks.funcs = append(hooks.funcs, f)
	return nil
}

// withAfterCommit gives the appliers of one label transaction a context that
// collects their AfterCommit hooks.
func withAfterCommit(ctx context.Context) (context.Context, *afterCommitHooks) {
	hooks := &afterCommitHooks{}
	return context.WithValue(ctx, afterCommitKey{}, hooks), hooks
}

// run calls every hook, after the transaction committed.
func (h *afterCommitHooks) run(ctx context.Context) {
	for _, f := range h.funcs {
		f(ctx)
	}
}
