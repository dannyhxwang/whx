package com.dsk.storm.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.kududb.Schema;
import org.kududb.client.*;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * User: yanbit
 * Date: 2015/12/16
 * Time: 17:47
 */
public class KuduState2<T> implements IBackingMap<T> {
    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = Maps.newHashMap();

    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    public static class Options<T> implements Serializable {
        public int localCacheSize = 5000;
        public String globalKey = "$KUDU__GLOBAL_KEY__$";
        public Serializer<T> serializer = null;
        public String tablename = "test_request_count";
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        if (keys.size() == 0) {
            System.out.println("----------------keys.none");
            return Collections.emptyList();
        }


        System.out.println("-----------keys.size()" + keys.size());
        List<String> allkeys = getAllKeys(keys);
        List<String> values = getAllValues(allkeys);

        return deserializeValues(keys, values);
    }

    private List<String> getAllValues(List<String> keys) {
        ArrayList<String> values = Lists.newArrayList();
        try {
            KuduTable table = kuduClient.openTable(options.tablename);
            kuduClient.newScannerBuilder(table);
            List<String> cols = new ArrayList<String>();
            cols.add("value");
            KuduClient client = new KuduClient.KuduClientBuilder("namenode").build();
            Schema schema = table.getSchema();

            for (String key : keys) {
                PartialRow start = schema.newPartialRow();
                start.addString("key", key);
                PartialRow end = schema.newPartialRow();
                end.addString("key", key + "1");
                KuduScanner scanner = client.newScannerBuilder(table)
                        .lowerBound(start)
                        .exclusiveUpperBound(end)
                        .setProjectedColumnNames(cols)
                        .build();
                while (scanner.hasMoreRows()) {
                    RowResultIterator results = scanner.nextRows();
                    while (results.hasNext()) {
                        RowResult result = results.next();
                        System.out.println(result.getString("value"));
                        values.add(result.getString("value"));
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return values;
    }

    private List<String> getAllKeys(List<List<Object>> keys) {
        List<String> values = new ArrayList<String>(keys.size());
        for (List<Object> key : keys) {
            if (key.size() != 1)
                throw new RuntimeException("Default KeyFactory does not support compound keys");
            values.add((String) key.get(0));
        }
        return values;
    }

    private List<T> deserializeValues(List<List<Object>> keys, List<String> values) {
        List<T> result = new ArrayList<T>(keys.size());
        for (String value : values) {
            if (value != null) {
                result.add((T) serializer.deserialize(value.getBytes()));
            } else {
                result.add(null);
            }
        }
        return result;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        KuduSession session = kuduClient.newSession();
        KuduTable table;
        try {
            table = kuduClient.openTable(options.tablename);
            Insert insert = table.newInsert();
            for (int i = 0; i < keys.size(); i++) {
                List<Object> list = keys.get(i);
                for (Object obj : list) {
                    System.out.println("------------------------put keys" + obj);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //
    private KuduClient kuduClient;
    private Options<T> options;
    private Serializer<T> serializer;

    public KuduState2(String hosts, Options<T> options, Serializer<T> serializer) {
        kuduClient = new KuduClient.KuduClientBuilder(hosts).build();
        this.options = options;
        this.serializer = serializer;
    }

    // factory
    protected static class Factory implements StateFactory {
        private StateType stateType;
        private Serializer serializer;
        private String hosts;
        private Options options;

        public Factory(StateType stateType, String hosts, Options options) {
            this.stateType = stateType;
            this.hosts = hosts;
            this.options = options;
            serializer = options.serializer;

            if (serializer == null) {
                serializer = DEFAULT_SERIALZERS.get(stateType);
            }

            if (serializer == null) {
                throw new RuntimeException("Serializer should be specified for type: " + stateType);
            }
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            KuduState2 state = new KuduState2(hosts, options, serializer);

            CachedMap cachedMap = new CachedMap(state, options.localCacheSize);

            MapState mapState;
            if (stateType == StateType.NON_TRANSACTIONAL) {
                mapState = NonTransactionalMap.build(cachedMap);
            } else if (stateType == StateType.OPAQUE) {
                mapState = OpaqueMap.build(cachedMap);
            } else if (stateType == StateType.TRANSACTIONAL) {
                mapState = TransactionalMap.build(cachedMap);
            } else {
                throw new RuntimeException("Unknown state type: " + stateType);
            }

            return new SnapshottableMap(mapState, new Values(options.globalKey));
        }
    }

    public static class DefaultKeyFactory implements KeyFactory {
        public String build(List<Object> key) {
            if (key.size() != 1)
                throw new RuntimeException("Default KeyFactory does not support compound keys");
            return (String) key.get(0);
        }
    }

    public static interface KeyFactory extends Serializable {
        public String build(List<Object> key);
    }

    public static StateFactory opaque(String hosts) {
        return opaque(hosts, new Options<OpaqueValue>());
    }

    public static StateFactory opaque(String hosts, Options<OpaqueValue> opts) {
        return new Factory(StateType.OPAQUE, hosts, opts);
    }

    public static StateFactory transactional(String hosts) {
        return transactional(hosts, new Options<TransactionalValue>());
    }

    public static StateFactory transactional(String hosts, Options<TransactionalValue> opts) {
        return new Factory(StateType.TRANSACTIONAL, hosts, opts);
    }

    public static StateFactory nonTransactional(String hosts) {
        return nonTransactional(hosts, new Options<Object>());
    }

    public static StateFactory nonTransactional(String hosts, Options<Object> opts) {
        return new Factory(StateType.NON_TRANSACTIONAL, hosts, opts);
    }
}
