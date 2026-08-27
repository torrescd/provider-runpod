FROM gcr.io/distroless/static@sha256:d9f9472a8f4541368192d714a995eb1a99bab1f7071fc8bde261d7eda3b667d8

ARG TARGETOS
ARG TARGETARCH

ADD bin/$TARGETOS\_$TARGETARCH/model-router /usr/local/bin/provider-runpod-model-router

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/provider-runpod-model-router"]
