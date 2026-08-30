FROM golang:1.26.6-alpine AS build

WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/google-messages-bridge .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/google-messages-bridge /google-messages-bridge
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/google-messages-bridge"]
