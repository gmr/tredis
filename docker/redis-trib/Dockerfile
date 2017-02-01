FROM gliderlabs/alpine:edge

RUN \
    apk update && \
    apk upgrade && \
    apk --update add ruby ruby-rake ruby-io-console ruby-bigdecimal ruby-json ruby-bundler \
    libstdc++ tzdata curl ca-certificates && \
    echo 'gem: --no-document' > /etc/gemrc && \
    gem install redis && \
    curl -o /usr/local/bin/redis-trib.rb https://raw.githubusercontent.com/antirez/redis/3.2/src/redis-trib.rb && \
    chmod a+x /usr/local/bin/redis-trib.rb && \
    apk --purge del curl && \
    rm -rf /var/cache/apk/*
ADD run.sh /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/run.sh"]
