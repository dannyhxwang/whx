package com.dsk.storm.service;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.dsk.storm.bolts.LogFilterBolt2;
import com.dsk.utils.Constants;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;

import java.util.Properties;
import java.util.UUID;

/**
 * kudu api
 */
public class LogProcess2 {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts(Constants.ZOOKEEPER_LIST);
        String zkRoot = "/" + "test";
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "test", zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        //spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        builder.setSpout("spout1", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt1", new LogFilterBolt2(), 10).shuffleGrouping("spout1");

        Config config = new Config();
        Properties props = new Properties();
        props.put("metadata.broker.list", Constants.BROKER_LIST);
        props.put("request.required.acks", "1");
        props.put("serializer.class", Constants.ENCODER);
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        config.put("kafka.spout.consumer.group", "test_UPUSERS");
//        config.setNumWorkers(2);
        config.setMaxSpoutPending(5000);
        config.setMessageTimeoutSecs(60);
        config.setNumAckers(3);
        StormSubmitter.submitTopology("testwhx_kudu", config, builder.createTopology());
    }
}
