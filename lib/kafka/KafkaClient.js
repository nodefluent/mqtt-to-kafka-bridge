"use strict";

const EventEmitter = require("events");
const Promise = require("bluebird");
const debug = require("debug")("mqtttokafka:kafka");
const { NProducer } = require("sinek");
const uuid = require("uuid");

class KafkaClient extends EventEmitter {

    constructor(config = {}, etl = null) {
        super();
        
        this.config = config;
        this.etl = etl;
        this.producer = null;
    }

    async connect(){
        debug("Connecting..");

        this.producer = new NProducer(this.config, "auto");
        
        this.producer.on("error", (error) => {
            this.emit("error", error);
        });

        await this.producer.connect();
        debug("Connected.");
    }

    produce(_topic, _message){

        const _key = uuid.v4();

        if(!this.etl){

            if(typeof _message !== "string"){
                _message = JSON.stringify(_message);
            }

            return this.producer.send(_topic, _message, null, _key);
        }

        return new Promise((resolve, reject) => {
            
            this.etl(_topic, _message, _key, (error, result) => {
                
                if(error){
                    return debug("Skipping message of", _topic, "as etl returned error.");
                }

                let {
                    topic,
                    message,
                    key,
                    partition = null,
                } = result;

                if(typeof message !== "string"){
                    message = JSON.stringify(message);
                }

                this.producer.send(topic, message, partition, key).then(resolve).catch(reject);
            });
        });
    }

    getStats(){
        return this.producer.getStats();
    }

    close(){

        if(this.producer){
            this.producer.close();
        }
    }
}

module.exports = KafkaClient;