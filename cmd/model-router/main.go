// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/torrescd/provider-runpod/apis"
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
	kube, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		log.Fatal("cannot create Kubernetes client")
	}
	handler, err := router.New(kube, *namespace)
	if err != nil {
		log.Fatal("cannot create model router")
	}
	ctx := ctrl.SetupSignalHandler()
	go handler.Run(ctx, 2*time.Second)

	server := &http.Server{
		Addr: *listen, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 10 * time.Minute, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 << 10,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("model-router listening on %s for one fail-closed route", *listen)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("model-router stopped unexpectedly")
	}
}
