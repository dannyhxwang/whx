package com.dsk.storm.test;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;

import java.util.UUID;

/**
 * An example Storm Trident topology that uses {@link HBaseAggregateState} for
 * stateful stream processing.
 * <p/>
 * This example persists idempotent counts in HBase for the number of times a
 * shortened URL has been seen in the stream for each day, week, and month.
 * <p/>
 * Assumes the HBase table has been created.<br>
 * <tt>create 'shorturl', {NAME => 'data', VERSIONS => 3},
 * {NAME => 'daily', VERSION => 1, TTL => 604800},
 * {NAME => 'weekly', VERSION => 1, TTL => 2678400},
 * {NAME => 'monthly', VERSION => 1, TTL => 31536000}</tt>
 */
public class TestUpmatch {
    /**
     * @param args
     * @throws InterruptedException
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AlreadyAliveException, AuthorizationException {
        BrokerHosts hosts = new ZkHosts("datanode1:2181,datanode2:2181,datanode4:2181");
        TridentKafkaConfig tridentKafkaConfig =
                new TridentKafkaConfig(hosts, "test_upmatch", UUID.randomUUID().toString());
        tridentKafkaConfig.ignoreZkOffsets = true;
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);

        TridentConfig config = new TridentConfig("upmatch", "key");
        config.setBatch(true);
        config.setFamily("info".getBytes());
        config.setColumns(Lists.newArrayList(
                "uid".getBytes(), "db".getBytes(), "tab".getBytes(), "rec".getBytes(), "ext".getBytes(),
                "ver".getBytes(), "nation".getBytes(), "pid".getBytes(), "count".getBytes()));
        StateFactory state = HBaseAggregateState.transactional(config);

        TridentTopology topology = new TridentTopology();
        topology
                .newStream("spout", tridentKafkaSpout)
                .each(new Fields("str"), new TestHBaseETL(), new Fields("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"))
                .project(new Fields("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"))
                .groupBy(new Fields("v1"))
                .persistentAggregate(state, new Fields("v9"), new Sum(), new Fields("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "count"));

        Config conf = new Config();
        conf.setNumWorkers(1);
        StormSubmitter.submitTopology("hbase-trident-test-upmatch", conf, topology.build());
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("hello", conf, topology.build());


    }
}
