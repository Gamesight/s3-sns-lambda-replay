FROM python:3.7-alpine

WORKDIR /opt/app
RUN addgroup -S -g 61000 replayuser && adduser -S -u 61000 -G replayuser replayuser
RUN apk update && \
    apk upgrade && \
    apk add git gcc build-base libxslt-dev linux-headers

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chown -R replayuser:replayuser .
USER replayuser:replayuser
