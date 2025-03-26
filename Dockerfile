# STEP 1
# Build app from source

FROM golang:1.24.1-alpine3.21 AS builder

WORKDIR /myapp

COPY ./cmd ./cmd

COPY ./vendor ./vendor
COPY ./go.mod ./go.sum ./.env ./

COPY ./internal ./internal

RUN go build -o app ./cmd/main.go

# STEP 2
# Container build

FROM alpine:3.21

WORKDIR /mysuperapp

COPY ./files ./files

COPY --from=builder /myapp/app ./

EXPOSE 7080

CMD ["./app"] 
