FROM gliderlabs/alpine:edge

RUN apk --update add redis &&  rm -rf /var/cache/apk/*
COPY run.sh /usr/local/bin/
VOLUME /data
WORKDIR /data
EXPOSE 6700 6701 6702 6703 6704 6705 6706 6707 6708 6709 6710 6711 6712 6713 6714 6715 6716
ENTRYPOINT ["/usr/local/bin/run.sh"]
