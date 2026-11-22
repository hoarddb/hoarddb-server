package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// verbStopContext is the context of a standalone verb's run. SIGINT and SIGTERM both
// cancel it, which is what cli/exitcodes.go promises for exit 4, and a run that sees it
// done stops at its next call, removes what it had half written and exits 4. After the
// first signal the registration is released, so a second one kills the process as usual.
func verbStopContext() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx, stop
}
