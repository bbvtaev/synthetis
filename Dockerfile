FROM golang:1.24 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o bin/service ./

FROM ubuntu:22.04

WORKDIR /

RUN apt update && apt install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/bin/service /usr/local/bin/service

CMD ["/usr/local/bin/service"]
