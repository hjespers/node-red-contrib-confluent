"use strict";
module.exports = function(RED) {
    var KafkaRest = require('kafka-rest');
    var util = require("util");

    function ConfluentProxyNode(n) {
        RED.nodes.createNode(this, n);
        this.proxy = n.proxy;
        this.clientid = n.clientid;
    }
    RED.nodes.registerType("rest-proxy", ConfluentProxyNode, {});

    function ConfluentInNode(n) {
        RED.nodes.createNode(this, n);
        this.topic = n.topic;
        this.proxy = n.proxy;
        this.cgroup = n.cgroup;
        this.autocommit = n.autocommit;
        this.proxyConfig = RED.nodes.getNode(this.proxy);
        var node = this;
        var stream;
        //var topic = this.topic;
        if (this.proxyConfig) {

            var kafka = new KafkaRest({ 'url': this.proxyConfig.proxy });

            // subscribe to kafka topic (if provided), otherwise print error message
            if (this.topic) {
                try {
                    kafka.consumer("my-consumer-group").join({
                        "format": "binary",
                        "auto.commit.enable": this.autocommit,
                    }, function(err, consumer_instance) {
                        // consumer_instance is a ConsumerInstance object
                        stream = consumer_instance.subscribe(node.topic);

                        stream.on('data', function(msgs) {
                            for(var i = 0; i < msgs.length; i++) {
                                console.log("Got a message: key=" + msgs[i].key + " value=" + msgs[i].value + " partition=" + msgs[i].partition);
                                var msg = {
                                    payload: msgs[i].value,
                                    topic: node.topic,
                                    offset: msgs[i].offset,
                                    key: msgs[i].key,
                                    partition: msgs[i].partition,
                                    size: msgs[i].size
                                };
                                try {
                                    node.send(msg);
                                } catch(e) {
                                    // statements
                                    util.log('[confluent] error sending node message: ' +e);
                                }
                            }
                        });
                        stream.on('error', function(err) {
                            console.error('[confluent] Error in our kafka input stream');
                            console.error(err);
                        });

 
                    });                
                } catch(e) {
                    util.log('[confluent] Error creating consumer:' +e);
                }
                util.log('[confluent] Created consumer on topic = ' + this.topic); 

                this.on('close', function() {
                    //cleanup
                    consumer_instance.shutdown(function() {
                        console.log("Shutdown complete.");
                    });
                });

            } else {
                this.error('missing input topic');
            }
        } else {
            this.error("missing proxy configuration");
        }

    }
    RED.nodes.registerType("confluent in", ConfluentInNode);

    function ConfluentOutNode(n) {
        RED.nodes.createNode(this, n);
        this.topic = n.topic;
        this.proxy = n.proxy;
        this.key = n.key;
        this.partition = n.partition;
        this.proxyConfig = RED.nodes.getNode(this.proxy);

        if (this.proxyConfig) {

            var kafka = new KafkaRest({ 'url': this.proxyConfig.proxy });

            // add request status in the future like Node-red HTTP nodes do
            // this.status({
            //    fill: "green",
            //    shape: "dot",
            //    text: "connected"
            // });

            this.on("input", function(msg) {
                if (msg == null || (msg.topic == "" && this.topic == "")) {
                    util.log("[confluent] request to send a NULL message or NULL topic on session: " + this.client.ref + " object instance: " + this.client[("_instances")]);
                } else if (msg != null && msg.topic != "" && this.topic == "") {
                    // use the topic specified on the message since one is not configured 

                    // add support for keys and partitions
                    // topic.produce({'key': 'key1', 'value': 'msg1', 'partition': 0}, function(err,res){});
                    kafka.topic(msg.topic).produce(msg.payload.toString(), function(err,res){
                        if (err) {
                            console.error('[confluent] Error publishing message to rest proxy');
                            console.error(err);
                        }
                    });

                } else if (msg != null && this.topic != "") {
                    // use the configures topic since it takes presedence
 
                    kafka.topic(this.topic).produce(msg.payload.toString(), function(err,res){
                        if (err) {
                            console.error('[confluent] Error publishing message to rest proxy');
                            console.error(err);
                        }
                    });
                }
            });
        } else {
            this.error("[confluent] missing proxy configuration");
        }
        this.on('close', function() {
            //cleanup
        });
    }
    RED.nodes.registerType("confluent out", ConfluentOutNode);

};