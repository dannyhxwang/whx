package com.dsk.storm.test;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import storm.trident.state.*;
import storm.trident.state.map.IBackingMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    private byte[] family;
    private List<byte[]> columns;
    private byte[] lastColumn;

    public HBaseAggregateState(TridentConfig config) {
        this.serializer = config.getStateSerializer();
        this.family = config.getFamily();
        this.columns = config.getColumns();
        this.lastColumn = (byte[]) config.getColumns().remove(columns.size() - 1);
        try {
            this.connector = new HTableConnector(config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        System.out.println("==================Get keys size==============" + keys.size());
        System.out.println("==================Get keys size==============" + keys.toString());
        List<Get> gets = new ArrayList<Get>(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            //[6b1966166806cc34ab0f4a1ff228e501, was3, webaddr_no, c2VhcmNoLm1wYy5hbQ==, LTEvfFBST0dSQU1GSUxFU3xcTVBDIENsZWFuZXJcTVBDVHJheS5leGU=, 5.9.63, jp, yacnvd]

            List<Object> lines = keys.get(i);
            System.out.println("+++++++++++++++++++++ rowkey" + lines.get(0).toString() + ":" + new String(lastColumn));
            byte[] rk = lines.get(0).toString().getBytes();
            Get g = new Get(rk);
            gets.add(g.addColumn(family, lastColumn));
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
            Result r = results[i];
            if (r.isEmpty()) {
                rtn.add(null);
            } else {
                System.out.print("--------------------------" + r.getValue(family, lastColumn));
                rtn.add((T) serializer.deserialize(r.getValue(family, lastColumn)));
            }
        }
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!" + rtn);
        return rtn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        List<Put> puts = new ArrayList<Put>();

        for (int i = 0; i < keys.size(); i++) {
            //[6b1966166806cc34ab0f4a1ff228e501, was3, webaddr_no, c2VhcmNoLm1wYy5hbQ==, LTEvfFBST0dSQU1GSUxFU3xcTVBDIENsZWFuZXJcTVBDVHJheS5leGU=, 5.9.63, jp, yacnvd]
            System.out.println("+++++++++++++++++++++ Put" + keys.get(i).toString());
            List<Object> lines = keys.get(i);
            byte[] rk = lines.get(0).toString().getBytes();

            if (columns.size() != 0) {
                for (int j = 1; j < lines.size(); j++) {
                    Put p = new Put(rk);
                    puts.add(p.add(family, columns.get(j - 1), lines.get(j).toString().getBytes()));
                }
            }
            Put p = new Put(rk);
            puts.add(p.add(family, lastColumn, serializer.serialize(vals.get(i))));
//            byte[] rk = Bytes.toBytes((String) keys.get(i).get(0));
//            byte[] cf = Bytes.toBytes("f");
//            byte[] cq = Bytes.toBytes("count");
            //byte[] cv = serializer.serialize(vals.get(i));
            //Put p = new Put(rk);
            //puts.add(p.add(cf, cq, cv));
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