# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.24 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

# go.mod first for layer caching. No external deps => no go.sum.
COPY go.mod ./
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/log-sink .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/log-sink /log-sink
EXPOSE 3100
USER nonroot
ENTRYPOINT ["/log-sink"]
