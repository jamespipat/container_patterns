# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.24 AS build
WORKDIR /src

# Dependency layer: cached unless go.mod/go.sum change. client-go is heavy.
COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/leader-elector .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/leader-elector /leader-elector
EXPOSE 4040
USER nonroot:nonroot
ENTRYPOINT ["/leader-elector"]
