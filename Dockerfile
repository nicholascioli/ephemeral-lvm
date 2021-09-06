FROM golang:alpine as builder

# Set up the build env
WORKDIR /build
COPY --chown=nobody:nobody go.mod go.sum /build/
COPY --chown=nobody:nobody cmd/ /build/cmd
COPY --chown=nobody:nobody pkg/ /build/pkg
USER nobody

# Build the application, rootless
RUN CGO_ENABLED=0 GOCACHE=/tmp/cache GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o ./cmd/elvm.bin ./cmd/elvm

# Create the actual image
FROM alpine:edge

# Install the needed dependencies
RUN apk add lvm2 lsblk xfsprogs e2fsprogs btrfs-progs

WORKDIR app
COPY --from=builder /build/cmd/elvm.bin /app/elvm

ENTRYPOINT ["/app/elvm"]
