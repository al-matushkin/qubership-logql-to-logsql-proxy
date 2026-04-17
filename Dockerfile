FROM golang:1.26.2-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /logql-proxy ./cmd/logql-proxy

FROM gcr.io/distroless/static-debian12
COPY --from=builder /logql-proxy /logql-proxy
EXPOSE 3100
ENTRYPOINT ["/logql-proxy"]
