# syntax=docker/dockerfile:1.7
# The LogDrop Go demo. Build from the repository root (nothing is pushed by this command):
#   docker build -f demo/Dockerfile -t logdrop-go-demo:dev .
FROM golang:1.26-alpine@sha256:ecb67f987aa1a57794c91d19ade7ae0209f2f0f5496aaef9e6ae85ddf92d02ab AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY logdropredis/go.mod logdropredis/go.sum logdropredis/
COPY demo/go.mod demo/go.sum demo/
RUN cd demo && GOWORK=off go mod download
COPY . .
RUN cd demo && GOWORK=off CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/logdrop-go-demo .

# Static binary on distroless: CA certificates for HTTPS to LogDrop, no shell, non-root.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/logdrop-go-demo /logdrop-go-demo
USER 10001:10001
EXPOSE 28120
ENTRYPOINT ["/logdrop-go-demo"]
