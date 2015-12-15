package com.dsk.storm.service;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.dsk.storm.function.PrintFunction;
import com.dsk.storm.function.RequestCountETL;
import com.dsk.utils.Constants;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

import java.util.UUID;

/**
 * User: yanbit
 * Date: 2015/12/15
 * Time: 10:10
 */
public class RequestCount {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // create TransactionalTridentKafkaSpout
        BrokerHosts hosts = new ZkHosts(Constants.ZOOKEEPER_LIST);
        TridentKafkaConfig tridentKafkaConfig =
                new TridentKafkaConfig(hosts, Constants.TOPIC_REQUEST_COUNT, UUID.randomUUID().toString());
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);

        // count request
        TridentTopology topology = new TridentTopology();
        Stream test = topology.newStream(Constants.TOPIC_REQUEST_COUNT, tridentKafkaSpout)
                .each(new Fields("raw"), new RequestCountETL(), new Fields("count"))
                .groupBy(new Fields("count"))
                .aggregate(new Count(), new Fields("result"))
                .each(new Fields("result"),new PrintFunction(),new Fields("print"));

        Config conf = new Config();
        conf.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(Constants.TOPIC_REQUEST_COUNT, conf, topology.build());
    }
}
