FROM golang:1.24.1-alpine3.21@sha256:43c094ad24b6ac0546c62193baeb3e6e49ce14d3250845d166c77c25f64b0386 AS builder

WORKDIR /build

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN CGO_ENABLED=0 go build -v -o ./conformance-posix ./cmd/conformance/posix

FROM alpine:3.20@sha256:0a4eaa0eecf5f8c050e5bba433f58c052be7587ee8af3e8b3910ef9ab5fbe9f5

COPY --from=builder /build/conformance-posix /

ENTRYPOINT ["/conformance-posix"]
