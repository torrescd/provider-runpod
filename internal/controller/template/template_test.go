// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package template

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
)

type fakeService struct {
	created   int
	createErr error
	recovered *clientrunpod.Template
}

func (f *fakeService) GetTemplate(context.Context, string) (*clientrunpod.Template, error) {
	return nil, clientrunpod.ErrNotFound
}
func (f *fakeService) FindTemplateByName(context.Context, string) (*clientrunpod.Template, error) {
	if f.recovered == nil {
		return nil, clientrunpod.ErrNotFound
	}
	return f.recovered, nil
}
func (f *fakeService) CreateTemplate(_ context.Context, in clientrunpod.TemplateInput) (*clientrunpod.Template, error) {
	f.created++
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &clientrunpod.Template{ID: "tpl_1", Name: in.Name, ImageName: in.ImageName, IsServerless: true}, nil
}
func (f *fakeService) UpdateTemplate(context.Context, string, clientrunpod.TemplateInput) (*clientrunpod.Template, error) {
	return nil, errors.New("not used")
}
func (f *fakeService) DeleteTemplate(context.Context, string) error { return clientrunpod.ErrNotFound }

func TestCreateRequiresDigestAndRecoversAmbiguousResult(t *testing.T) {
	cr := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "model"}, Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{Name: "model", ImageName: "registry/model:latest"}}}
	svc := &fakeService{}
	e := &external{service: svc}
	if _, err := e.Create(context.Background(), cr); err == nil || svc.created != 0 {
		t.Fatal("mutable image tag was accepted")
	}
	cr.Spec.ForProvider.ImageName = "registry/model@sha256:" + strings.Repeat("a", 64)
	svc.createErr = clientrunpod.ErrCreateAmbiguous
	svc.recovered = &clientrunpod.Template{ID: "tpl_recovered", Name: "model", ImageName: cr.Spec.ForProvider.ImageName, IsServerless: true}
	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if meta.GetExternalName(cr) != "tpl_recovered" {
		t.Fatalf("external name=%q", meta.GetExternalName(cr))
	}
}

func TestDelete404IsSuccess(t *testing.T) {
	cr := &serverlessv1alpha1.Template{}
	meta.SetExternalName(cr, "gone")
	if _, err := (&external{service: &fakeService{}}).Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
}
