# Local did:plc directory (the real did-method-plc server) baked into an
# image at build time. The Coves/Tidepool dev composes clone + build the
# repo inside a node container on first START (cached in a volume); for the
# e2e stack that would either survive `down -v` poorly or rebuild for
# minutes on every run, so the clone/build moves into the image where the
# docker layer cache owns it.
#
# The clone is pinned to PLC_COMMIT so local (layer-cached) and CI (cold)
# builds compile the same source. To bump: take the SHA from
#   git ls-remote https://github.com/did-method-plc/did-method-plc.git main
# update PLC_COMMIT below, and rebuild
#   (docker compose -f docker-compose.e2e.yml build plc).

FROM node:18-alpine

RUN apk add --no-cache git python3 make g++

ARG PLC_COMMIT=2ed82a5ccf1b424aa5e2f6c5b461dc0ee133278b

WORKDIR /app
RUN git init -q . \
    && git remote add origin https://github.com/did-method-plc/did-method-plc.git \
    && git fetch --depth 1 origin "${PLC_COMMIT}" \
    && git checkout -q FETCH_HEAD \
    && yarn install --frozen-lockfile \
    && yarn build

WORKDIR /app/packages/server

ENV PORT=3000
EXPOSE 3000
CMD ["yarn", "start"]
