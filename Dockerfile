# Multi-stage build: compile a fully static binary, then ship it in a tiny image.

# --- build stage ---
FROM golang:1.26 AS build
WORKDIR /src

# Copy go.mod first so dependency resolution is cached separately from source.
# (This project uses only the standard library, so there is nothing to download.)
COPY go.mod ./
COPY . .

# CGO_ENABLED=0 produces a static binary with no libc dependency, so it runs in
# a scratch/distroless image. -ldflags "-s -w" strips debug info to shrink it.
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -ldflags="-s -w" -o /out/gateway ./cmd/gateway

# --- runtime stage ---
# distroless/static has no shell and no package manager — minimal attack surface.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/gateway /gateway

# The HTTP API listens here by default (override with -addr).
EXPOSE 8080

# Default to serving the API. Override the command for the CLI demo, e.g.:
#   docker run --rm llmgateway embed -n 2000 -workers 32
ENTRYPOINT ["/gateway"]
CMD ["serve", "-addr", ":8080"]
