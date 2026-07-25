# syntax=docker/dockerfile:1

# --- build stage (cross-compiles for the target platform) ---
FROM --platform=$BUILDPLATFORM golang:1.24 AS build
WORKDIR /src

# Dependencies first for layer caching.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/order-api .

# --- runtime stage ---
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/order-api /order-api
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/order-api"]
