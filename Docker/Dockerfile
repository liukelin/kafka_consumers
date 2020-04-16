FROM golang:1.12-alpine as builder

ENV LIBRDKAFKA_VERSION 1.4.0
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

RUN apk add --no-cache --virtual .fetch-deps \
    bash              \
    git 				\
    musl-dev          \
    zlib-dev    \
    openssl-dev

RUN cd /tmp \
    && git clone --branch ${LIBRDKAFKA_VERSION} https://github.com/edenhill/librdkafka.git \
    && cd /tmp/librdkafka \
    && ./configure --install-deps --prefix /usr \
    && make \
    && make install \
    && cd \
    && rm -rf /tmp/librdkafka \
    && apk del .fetch-deps
