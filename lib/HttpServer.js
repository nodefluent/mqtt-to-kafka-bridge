"use strict";

const Promise = require("bluebird");
const debug = require("debug")("mqtttokafka:http");
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");

class HttpServer {

    constructor(config = {}, bridge = null){
        this.config = config;
        this.bridge = bridge;
        this.server = null;
    }

    run(){

        debug("Starting..");

        const app = express();

        app.use(cors());
        app.use(bodyParser.json({extended: false}));

        app.get("/", (req, res) => {
            res.json({
                "self": "/",
                "Healthcheck": "/healthcheck",
                "Bridge Status": "/stats"
            });
        });

        app.get("/healthcheck", (req, res) => {
            res.status(200).end();
        });

        app.get("/stats", (req, res) => {
            res.json(this.bridge.getStats());
        });

        return new Promise((resolve, reject) => {
            this.server = app.listen(this.config.port, (error) => {
                
                if(error){
                    return reject(error);
                }

                debug(`Listening @ http://localhost:${this.config.port}`);
                resolve(this);
            });
        });
    }

    close(){

        if(this.server){
            this.server.close();
        }
    }
}

module.exports = HttpServer;