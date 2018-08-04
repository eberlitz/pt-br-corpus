FROM python:3.6
MAINTAINER Eduardo Eidelwein Berlitz "eberlitz@gmail.com"

USER root

WORKDIR /usr/src/app

ADD ./word2vecf.tar ./

WORKDIR /usr/src/app/word2vecf
RUN make
WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./scripts ./scripts
