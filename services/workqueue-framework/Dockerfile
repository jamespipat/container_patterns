# syntax=docker/dockerfile:1

# ---- build stage: cross-compile a static binary ----
FROM --platform=$BUILDPLATFORM golang:1.24 AS build
WORKDIR /src

# Cache modules first for faster rebuilds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/framework .

# ---- runtime stage: distroless, non-root ----
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/framework /framework
USER nonroot
EXPOSE 8091
ENTRYPOINT ["/framework"]
