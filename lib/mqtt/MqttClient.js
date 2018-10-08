"use strict";

const EventEmitter = require("events");
const debug = require("debug")("mqtttokafka:mqtt");
const mqtt = require("mqtt");

class MqttClient extends EventEmitter {

    constructor(config = {}, etl = null) {
        super();

        this.config = config;
        this.etl = etl;
        this.client = null;
    }

    async connect(){
        debug("Connecting..");

        let {
            url,
            options = {}
        } = this.config;

        options = Object.assign({}, options, {
            reconnectPeriod: 1000,
            resubscribe: true
        });

        this.client = mqtt.connect(url, options);

        this.client.on("error", (error) => {
            this.emit("error", error);
        });

        this.client.on("offline", () => {
            debug("is offline");
        });

        this.client.on("reconnect", () => {
            debug("is reconnecting");
        });

        this.client.on("end", () => {
            debug("ended");
        });

        this.client.on("message", (_topic, _message, _packet) => {

            if(_message && Buffer.isBuffer(_message)){
                _message = _message.toString("utf8");
            }

            if(_message && typeof _message === "string"){
                try {
                    const parsed = JSON.parse(_message);
                    _message = parsed;
                } catch(_){
                    // empty
                }
            }

            if(this.etl){
                this.etl(_topic, _message, _packet, (error, result) => {

                    if(error){
                        return  debug("Skipping message of", _topic, "as etl returned error.");
                    }

                    const {
                        topic,
                        message,
                    } = result;

                    this.emit("message", topic, message);
                });
            } else {
                this.emit("message", _topic, _message);
            }
        });

        // only kafka connection awaiting is required, we can ignore this
        this.client.on("connect", () => {
            debug("Connected.");

            debug("Subscribing to topics..");
            this.client.subscribe("#", (error, qos) => {

                if(error){
                    debug("Failed to subscribe to topics.");
                    this.emit("error", error);
                }

                debug("Subscribed to topics.");
            });
        });
    }

    getStats(){
        return {
            connected: this.client.connected,
        };
    }

    close(){

        if(this.client){
            this.client.end();
        }
    }
}

module.exports = MqttClient;