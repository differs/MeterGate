# MeterGate — multi-stage build (Go 1.25, distroless runtime)
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -buildvcs=false -o /metergate ./cmd/metergate \
    && CGO_ENABLED=0 go build -buildvcs=false -o /reconcile ./cmd/reconcile

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /metergate /metergate
COPY --from=build /reconcile /reconcile
EXPOSE 3000
ENTRYPOINT ["/metergate"]
