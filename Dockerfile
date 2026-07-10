# Tidepool — multi-stage build, small static final image.
#
# The binary is fully static (CGO_ENABLED=0; lib/pq is pure Go) and goose
# migrations are embedded, so the final stage needs nothing but CA certs
# (outbound https in production) and busybox wget for compose healthchecks.
# Runs as a non-root user; docker's default ip_unprivileged_port_start=0
# lets it bind :80 (the e2e harness serves the bridge portless at
# http://tidepool/ so AP object ids stay clean hostnames).

FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependency layer first so code changes don't re-download modules.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/tidepool ./cmd/tidepool

FROM alpine:3.21

RUN apk add --no-cache ca-certificates \
    && adduser -D -u 1000 tidepool

COPY --from=build /out/tidepool /usr/local/bin/tidepool

USER tidepool

EXPOSE 80
ENTRYPOINT ["tidepool"]
