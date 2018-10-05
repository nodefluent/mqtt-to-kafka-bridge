# mqtt-to-kafka-bridge - consume, (etl/mirror), produce

[![npm version](https://badge.fury.io/js/mqtt-to-kafka-bridge.svg)](https://badge.fury.io/js/mqtt-to-kafka-bridge)
[![Docker Repository on Quay](https://quay.io/repository/nodefluent/mqtt-to-kafka-bridge/status "Docker Repository on Quay")](https://quay.io/repository/nodefluent/mqtt-to-kafka-bridge)

## Intro

`mqtt-to-kafka-bridge` allows you to quickly setup a fast (about messages 2 million/sec) and lightweight (about 100 MB RAM)
bridge that subscribes to your MQTT Broker and produces messages to your Apache Kafka cluster.
You can configure **routing** (move messags from MQTT topics to certain Kafka topics) or filtering, as well as **ETL functions**
on MQTT consume, as well as on Kafka produce. You just pass everything in a simple JSON/JS config object.

## How to use?

Just check out the example [here](example/sample.js) it also gives you description on the configuration options.
You can find the sample configuration [here](example/config.js).

## Info

The bridge spins up an http server, which can be used to check its health status `http://localhost:3967/healthcheck` as well as statistics `http://localhost:3967/stats`.

## Maintainer

Build with :heart: :pizza: and :coffee: by [nodefluent](https://github.com/nodefluent)
