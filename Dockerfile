FROM alpine

COPY gopath/bin/firehose /firehose

ENTRYPOINT ["/firehose"]
