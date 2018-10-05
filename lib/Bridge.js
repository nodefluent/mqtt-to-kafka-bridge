"use strict";

const EventEmitter = require("events");
const debug = require("debug")("mqtttokafka:bridge");
const KafkaClient = require("./kafka/KafkaClient.js");
const MqttClient = require("./mqtt/MqttClient.js");
const HttpServer = require("./HttpServer.js");

const MQTT_TOPIC_PATTERN = new RegExp("/", "g");
const WILDCARD = "*";

class Bridge extends EventEmitter {

    constructor(config = {}){
        super();

        if(!config.kafka){
            throw new Error("Kafka configuration missing.");
        }

        if(!config.mqtt){
            throw new Error("MQTT configuration missing.");
        }

        if(!config.routing){
            throw new Error("Routing configuration missing.");
        }

        debug("Routing configuration", config.routing);

        this.config = config;
        this.topicDelimiter = config.kafkaTopicDelimiter || "-";
        this.routedMessages = 0;
        this.skippedMessages = 0;
        this.errors = 0;
        this.startedAt = (new Date()).toISOString();

        this.on("error", (error) => {
            this.errors++;
        });

        this.mqttClient = new MqttClient(config.mqtt, config.subscribeEtl);

        this.mqttClient.on("error", (error) => {
            this.emit("error", error);
        });

        this.mqttClient.on("message", (topic, message) => {
            
            if(this.config.logMessages){
                debug("routing for topic", topic, message);
            }

            this._route(topic, message, this.config.routing);
        });

        this.kafkaClient = new KafkaClient(config.kafka, config.produceEtl);

        this.kafkaClient.on("error", (error) => {
            this.emit("error", error);
        });

        this.httpServer = new HttpServer(config.http || { port: 3967 }, this);
    }

    _route(topic, message, routing){

        // we try to use specific topic routings first
        let target = routing[topic];

        // otherwise check if there is a wildcard configured
        if(!target){
            target = routing[WILDCARD];
        }
        
        // mqtt topic not configured and wildcard not present, we drop this message
        if(!target){
            this.skippedMessages++;
            return;
        }

        this.routedMessages++;

        // if target for this topic is a wildcard, we have to convert the mqtt topic name to a kafka topic
        if(target === WILDCARD){
            const kafkaTopicName = topic.replace(MQTT_TOPIC_PATTERN, this.topicDelimiter);
            return this.kafkaClient
                .produce(kafkaTopicName, message)
                .catch((error) => { this.emit("error", error); });
        }

        // if target is not a wildcard, target is the topic name to produce this message to
        return this.kafkaClient
            .produce(target, message)
            .catch((error) => { this.emit("error", error); });
    }

    async run(){
        debug("Starting..");
        await this.kafkaClient.connect();
        await this.mqttClient.connect();
        await this.httpServer.run();
        debug("Started.");
    }

    getStats(){
        return {
            startedAt: this.startedAt,
            bridge: {
                skippedMessages: this.skippedMessages,
                routedMessages: this.routedMessages,
                errorCount: this.errors,
            },
            mqtt: this.mqttClient.getStats(),
            kafka: this.kafkaClient.getStats(),
        };
    }

    close(){
        
        debug("Closing..");

        if(this.httpServer){
            this.httpServer.close();
        }
        
        if(this.mqttClient){
            this.mqttClient.close();
        }

        if(this.kafkaClient){
            this.kafkaClient.close();
        }
    }
}

module.exports = Bridge;