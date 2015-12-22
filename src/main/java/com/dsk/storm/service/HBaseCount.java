package com.dsk.storm.service;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.dsk.storm.state.HBaseAggregateState;
import com.dsk.utils.Constants;
import com.dsk.utils.TridentConfig;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;

import java.util.UUID;

/**
 * User: yanbit
 * Date: 2015/12/15
 * Time: 10:10
 */
public class HBaseCount {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // create TransactionalTridentKafkaSpout
        BrokerHosts hosts = new ZkHosts(Constants.ZOOKEEPER_LIST);
        TridentKafkaConfig tridentKafkaConfig =
                new TridentKafkaConfig(hosts, Constants.TOPIC_REQUEST_COUNT, UUID.randomUUID().toString());
        tridentKafkaConfig.ignoreZkOffsets=true;
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);

//        // Redis state
//        StateFactory state = RedisState.transactional(new InetSocketAddress("namenode", 6379), Constants.TOPIC_UPMATCH);
////        StateFactory state = KuduState2.transactional("namenode");
//        // count request
//        TridentTopology topology = new TridentTopology();
//        TridentState test = topology.newStream(Constants.TOPIC_UPMATCH, tridentKafkaSpout)
//                .each(new Fields("str"), new HBaseCountETL(), new Fields("rowkey"))
//                .groupBy(new Fields("rowkey"))
//                .persistentAggregate(state, new Count(), new Fields("count"));

                //.each(new Fields("word","count"),new PrintFunction(),new Fields("print"));

        TridentConfig config = new TridentConfig("test", "key");
        config.setStateCacheSize(5000);
        config.addColumn("f", "count");
        StateFactory state = HBaseAggregateState.transactional(config);
        TridentTopology topology = new TridentTopology();
        topology
                .newStream("spout", tridentKafkaSpout)
                .groupBy(new Fields("key"))
                .persistentAggregate(state, new Count(), new Fields("count"));
        conf.setNumWorkers(1);
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("test_state",conf,topology.build());
        StormSubmitter.submitTopologyWithProgressBar("test_hbase_v1", conf, topology.build());
    }
}
