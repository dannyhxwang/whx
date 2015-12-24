package com.dsk.storm.test;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import storm.trident.state.*;
import storm.trident.state.map.IBackingMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A HBase persistentAggregate source of state for Storm Trident topologies
 *
 * @param <T> The type of value being persisted. Either {@link OpaqueValue} or
 *            {@link TransactionalValue}
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class HBaseAggregateState<T> implements IBackingMap<T> {
    /**
     * @param config The {@link TridentConfig}
     * @return {@link StateFactory} for opaque transactional topologies
     */
    public static StateFactory opaque(TridentConfig<OpaqueValue> config) {
        return new HBaseAggregateFactory(config, StateType.OPAQUE);
    }

    /**
     * @param config The {@link TridentConfig}
     * @return {@link StateFactory} for transactional topologies
     */
    public static StateFactory transactional(TridentConfig<TransactionalValue> config) {
        return new HBaseAggregateFactory(config, StateType.TRANSACTIONAL);
    }

    /**
     * @param config The {@link TridentConfig}
     * @return {@link StateFactory} for non-transactional topologies
     */
    public static StateFactory nonTransactional(TridentConfig<TransactionalValue> config) {
        return new HBaseAggregateFactory(config, StateType.NON_TRANSACTIONAL);
    }

    private HTableConnector connector;
    private Serializer serializer;
    private String family;
    private Set<String> columns;

    public HBaseAggregateState(TridentConfig config) {
        this.serializer = config.getStateSerializer();
        try {
            this.connector = new HTableConnector(config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Map<String, Set<String>> map = config.columnFamilies;
        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
            this.family = entry.getKey();
            this.columns = entry.getValue();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        System.out.println("==================keys size==============" + keys.size());
        System.out.println("==================keys size==============" + keys.toString());
        List<Get> gets = new ArrayList<Get>(keys.size());
        byte[] rk;
        byte[] cf;
        byte[] cq;

        for (List<Object> k : keys) {
            System.out.println("+++++++++++++++++++++ Get" + k.toString());
            rk = Bytes.toBytes((String) k.get(0));
//            cf = Bytes.toBytes((String) k.get(1));
//            cq = Bytes.toBytes((String) k.get(2));
            cf = Bytes.toBytes("f");
            cq = Bytes.toBytes("count");
            Get g = new Get(rk);
            gets.add(g.addColumn(cf, cq));
        }

        // Log.debug("GETS: " + gets.toString());

        Result[] results = null;
        try {
            results = connector.getTable().get(gets);
        } catch (IOException e) {
            new RuntimeException(e);
        }

        List<T> rtn = new ArrayList<T>(keys.size());

        for (int i = 0; i < keys.size(); i++) {
//            cf = Bytes.toBytes((String) keys.get(i).get(1));
//            cq = Bytes.toBytes((String) keys.get(i).get(2));
            cf = Bytes.toBytes("f");
            cq = Bytes.toBytes("count");
            Result r = results[i];
            if (r.isEmpty()) {
                rtn.add(null);
            } else {
                rtn.add((T) serializer.deserialize(r.getValue(cf, cq)));
            }
        }

        return rtn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        List<Put> puts = new ArrayList<Put>();

        for (int i = 0; i < keys.size(); i++) {
            System.out.println("+++++++++++++++++++++ Put" + keys.get(i).toString());
            byte[] rk = Bytes.toBytes((String) keys.get(i).get(0));
            byte[] cf = Bytes.toBytes("f");
            byte[] cq = Bytes.toBytes("count");
            byte[] cv = serializer.serialize(vals.get(i));
            Put p = new Put(rk);
            puts.add(p.add(cf, cq, cv));
        }

        // Log.debug("PUTS: " + puts.toString());

        try {
            connector.getTable().put(puts);
            connector.getTable().flushCommits();
        } catch (IOException e) {
            new RuntimeException(e);
        }
    }
}