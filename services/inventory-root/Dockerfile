# syntax=docker/dockerfile:1

# ---- build stage: cross-compile a static binary ----
FROM --platform=$BUILDPLATFORM golang:1.24 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

# Copy the module manifest first for layer caching. There are no third-party
# dependencies, so no go.sum is needed.
COPY go.mod ./
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags="-s -w" -o /out/inventory-root .

# ---- runtime stage: distroless, nonroot ----
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/inventory-root /inventory-root
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/inventory-root"]
