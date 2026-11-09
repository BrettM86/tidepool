# Reverse proxy in front of the e2e did:plc directory; see main.go. Same shape
# as the root Dockerfile: a static binary on alpine, which keeps busybox wget
# for the compose healthcheck.

FROM golang:1.27.3-alpine3.24 AS build

WORKDIR /src

COPY go.mod main.go ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/plc-fixture .

FROM alpine:3.24.1

COPY --from=build /out/plc-fixture /usr/local/bin/plc-fixture

USER nobody

EXPOSE 3000 3001
ENTRYPOINT ["plc-fixture"]
