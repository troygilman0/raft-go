FROM golang:1.22

WORKDIR /app

COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /raft-go

CMD ["/raft-go"]