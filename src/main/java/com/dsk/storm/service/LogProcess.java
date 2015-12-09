package com.dsk.storm.service;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.dsk.storm.bolts.LogFilterBolt;
import com.dsk.utils.Constants;
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
        BrokerHosts hosts = new ZkHosts(Constants.ZOOKEEPER_LIST);
        String zkRoot = "/" + Constants.TOPIC;
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(hosts, Constants.TOPIC, zkRoot, "XXX");
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        builder.setSpout("spout1", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt1", new LogFilterBolt(), 10).shuffleGrouping("spout1");

        Config config = new Config();
        Properties props = new Properties();
        props.put("metadata.broker.list", Constants.BROKER_LIST);
        props.put("request.required.acks", "1");
        props.put("serializer.class", Constants.ENCODER);
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
//        config.setNumWorkers(2);
        config.setMaxSpoutPending(5000);
        config.setMessageTimeoutSecs(60);
        config.setNumAckers(3);
        StormSubmitter.submitTopology("testwhx", config, builder.createTopology());
    }
}
