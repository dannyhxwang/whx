package com.dsk.storm.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.kududb.client.Insert;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.Serializable;
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
        public int localCacheSize = 1000;
        public String globalKey = "$KUDU__GLOBAL_KEY__$";
        public Serializer<T> serializer = null;
        public String tablename = "test_wordcount";
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        if (keys.size()==0){
            return Collections.emptyList();
        }

//        Map<byte[], byte[]> keyValue = hgetAll(this.options.hkey.getBytes()); get all keys
//        List<String> values = buildValuesFromMap(keys, keyValue); get values by keys
//        private List<String> buildValuesFromMap(List<List<Object>> keys, Map<byte[], byte[]> keyValue) {
//            List<String> values = new ArrayList<String>(keys.size());
//            for (List<Object> key : keys) {
//                String strKey = keyFactory.build(key);
//                byte[] bytes = keyValue.get(strKey.getBytes());
//                values.add(bytes == null ? null : new String(bytes));
//            }
//            return values;
//        }
//        return deserializeValues(keys, values);

        ///////////////////////////////
        System.out.println("-----------keys.size()" + keys.size());
        List<T> slist = Lists.newArrayList();
        for (List<Object> lobj : keys){
            System.out.println("-----------lobj---------keys.size()"+lobj.size());
            for (Object obj : lobj){
                System.out.println("-------------all keys:"+obj);
                slist.add((T)obj);
            }
        }

        return slist;
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
                for (Object  obj : list){
                    System.out.println("------------------------put keys"+obj);
                }
//                Composite columnName = toColumnName(keys.get(i));
//                byte[] bytes = serializer.serialize(values.get(i));
//                HColumn<Composite, byte[]> column = HFactory.createColumn(columnName, bytes);
//                mutator.insert(options.rowKey, options.columnFamily, column);
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
