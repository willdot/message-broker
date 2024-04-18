FROM golang:latest as builder

WORKDIR /my-app

COPY go.mod go.sum ./
COPY cmd/server/ ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o message-broker-server .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /my-app/message-broker-server .
EXPOSE 3000
CMD ["./message-broker-server"]
