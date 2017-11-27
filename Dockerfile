FROM daocloud.io/library/ubuntu:16.04

MAINTAINER Jiawen Guan <gjw.jesus@qq.com>

WORKDIR /pentadb

ADD . /pentadb

ENV GOPATH=/pentadb

RUN apt update && apt install golang -y

RUN chmod +x /pentadb/entry.sh

CMD . /pentadb/entry.sh