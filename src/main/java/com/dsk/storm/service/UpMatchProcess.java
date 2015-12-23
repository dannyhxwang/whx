package com.dsk.storm.service;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.dsk.storm.bolts.UpMatchCountBolt;
import com.dsk.storm.bolts.UpMatchETLBolt;
import com.dsk.utils.Constants;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by wanghaixing
 * on 2015/12/18 15:25.
 */
public class UpMatchProcess {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts(Constants.ZOOKEEPER_LIST);
        String zkRoot = "/" + Constants.TOPIC_UPMATCH;
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(hosts, Constants.TOPIC_UPMATCH, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        builder.setSpout("upmatch_spout", new KafkaSpout(spoutConfig));
        builder.setBolt("upmatch_etl", new UpMatchETLBolt(), 3).fieldsGrouping("upmatch_spout", new Fields("rowkey"));
        builder.setBolt("upmatch_count", new UpMatchCountBolt(), 3).shuffleGrouping("upmatch_etl");

                Config config = new Config();
        Properties props = new Properties();
        props.put("metadata.broker.list", Constants.BROKER_LIST);
        props.put("request.required.acks", "1");
        props.put("serializer.class", Constants.ENCODER);
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
//        config.setNumWorkers(2);
        config.setMaxSpoutPending(5000);
        config.setMessageTimeoutSecs(60);
//        config.setNumAckers(3);
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
        StormSubmitter.submitTopology("testupmatch", config, builder.createTopology());
    }
}
