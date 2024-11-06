FROM golang:1.20.5 as builder

RUN apt-get update && apt-get install -y

WORKDIR /kalyan3104
COPY . .

WORKDIR /kalyan3104/cmd/elasticindexer

RUN go build -o elasticindexer

# ===== SECOND STAGE ======
FROM ubuntu:22.04
RUN apt-get update && apt-get install -y

RUN useradd -m -u 1000 appuser
USER appuser

COPY --from=builder /kalyan3104/cmd/elasticindexer /kalyan3104

EXPOSE 22111

WORKDIR /kalyan3104

ENTRYPOINT ["./elasticindexer"]
CMD ["--log-level", "*:DEBUG"]
