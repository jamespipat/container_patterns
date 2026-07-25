# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.24 AS build
WORKDIR /src

# No third-party dependencies (stdlib only), so there is nothing to download;
# copying go.mod first keeps the layer cache stable if deps are ever added.
COPY go.mod ./
COPY *.go ./

# Run the built-in self-test (RESP framing + routing) on the build platform.
RUN go vet ./... && go test ./...

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/cache-ambassador .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/cache-ambassador /cache-ambassador
USER nonroot
EXPOSE 6380
ENTRYPOINT ["/cache-ambassador"]
