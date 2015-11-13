package com.dsk.storm.service;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.dsk.storm.bolts.LogFilterBolt;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by wanghaixing on 2015/11/12.
 */
public class LogProcess {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts("datanode1:2181,datanode2:2181,datanode4:2181");
        String topic = "test_upusers";
        String zkRoot = "/test_upusers";
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        builder.setSpout("spout1", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt1", new LogFilterBolt()).shuffleGrouping("spout1");

        Config config = new Config();
        Properties props = new Properties();
        props.put("metadata.broker.list", "10.1.3.55:9092,10.1.3.56:9092,10.1.3.59:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        config.setNumWorkers(2);
        StormSubmitter.submitTopology("testlog", config, builder.createTopology());
    }
}
