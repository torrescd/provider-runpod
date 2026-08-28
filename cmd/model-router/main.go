// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/torrescd/provider-runpod/apis"
	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	"github.com/torrescd/provider-runpod/internal/controller/endpointcheck"
	"github.com/torrescd/provider-runpod/internal/router"
)

func main() {
	listen := flag.String("listen", ":8080", "listen address")
	namespace := flag.String("namespace", os.Getenv("POD_NAMESPACE"), "single namespace containing EndpointChecks")
	flag.Parse()
	if *namespace == "" {
		log.Fatal("namespace is required")
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		log.Fatal("cannot register core API")
	}
	if err := apis.AddToScheme(scheme); err != nil {
		log.Fatal("cannot register provider API")
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{
			*namespace: {},
		}},
		Client: client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{
			&corev1.Secret{},
			&serverlessv1alpha1.Endpoint{},
		}}},
		Metrics:                server.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
	})
	if err != nil {
		log.Fatal("cannot create namespace-scoped controller manager")
	}
	if err := endpointcheck.Setup(mgr); err != nil {
		log.Fatal("cannot setup EndpointCheck controller")
	}
	handler, err := router.New(mgr.GetClient(), *namespace, router.WithAPIReader(mgr.GetAPIReader()))
	if err != nil {
		log.Fatal("cannot create model router")
	}
	signalContext := ctrl.SetupSignalHandler()
	ctx, cancel := context.WithCancel(signalContext)
	defer cancel()
	go handler.Run(ctx, 2*time.Second)

	server := &http.Server{
		Addr: *listen, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 10 * time.Minute, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 << 10,
	}
	managerStopped := make(chan error, 1)
	go func() { managerStopped <- mgr.Start(ctx) }()
	serverStopped := make(chan error, 1)
	log.Printf("model-router listening on %s for one fail-closed route", *listen)
	go func() { serverStopped <- server.ListenAndServe() }()

	var stoppedErr error
	select {
	case stoppedErr = <-managerStopped:
	case stoppedErr = <-serverStopped:
	case <-signalContext.Done():
	}
	cancel()
	shutdown, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdown)
	if stoppedErr != nil && !errors.Is(stoppedErr, http.ErrServerClosed) {
		log.Fatal("model-router stopped unexpectedly")
	}
}
