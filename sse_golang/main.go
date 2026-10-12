package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	// Ctrl-C / SIGTERM → cancel ctx → producers stop, request contexts die.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := NewBroker()
	startStocks(ctx, broker)
	startMetrics(ctx, broker)
	startNotifications(ctx, broker)

	s := &server{broker: broker, started: time.Now()}
	srv := &http.Server{
		Addr:    *addr,
		Handler: routes(s),
		// WriteTimeout MUST stay 0: a non-zero value kills long-lived SSE
		// streams. The SSE handler sets per-write deadlines instead.
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		// Tie request lifetimes to our shutdown ctx so SSE handlers exit
		// immediately on shutdown instead of blocking srv.Shutdown.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	go func() {
		log.Printf("SSE demo listening on %s, open http://localhost%s", *addr, *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	log.Printf("bye (stats: %+v)", broker.Stats())
}
