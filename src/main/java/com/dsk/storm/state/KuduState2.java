package com.dsk.storm.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.kududb.Schema;
import org.kududb.client.*;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

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

        System.out.println("---------------multiGet start--------------" +
                new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date()));
        if (keys.size() == 0) {
            return Collections.emptyList();
        }

        List<String> allkeys = getAllKeys(keys);
        List<String> values = getAllValues(allkeys);

        System.out.println("---------------multiGet end--------------" +
                new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date()));
        return deserializeValues(keys, values);
    }

    private List<String> getAllValues(List<String> keys) {
        System.out.println("---------------get all value start--------------" +
                new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date()));
        ArrayList<String> values = Lists.newArrayList();
        try {
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
                String value = null;
                while (scanner.hasMoreRows()) {
                    RowResultIterator results = scanner.nextRows();
                    while (results.hasNext()) {
                        RowResult result = results.next();
                        value = result.getString("value");
                    }
                }
                values.add(value);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("---------------get all value end--------------" +
                new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date()));
        return values;
    }

    private List<String> getAllKeys(List<List<Object>> keys) {
        List<String> values = new ArrayList<String>(keys.size());
        for (List<Object> key : keys) {
            if (key.size() != 1) {
                throw new RuntimeException("Default KeyFactory does not support compound keys");
            }
            values.add((String) key.get(0));
        }
        return values;
    }


    private List<T> deserializeValues(List<List<Object>> keys, List<String> values) {
        System.out.println("------------deserialize value start ");
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
        System.out.println("---------------multiPut start--------------" +
                new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date()));
        if (keys.size() == 0) {
            return;
        }

        Table<String, String, String> aTable = HashBasedTable.create();

        //set flush size
        session.setMutationBufferSpace(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            String key = (String) keys.get(i).get(0);
            String val = new String(serializer.serialize(vals.get(i)));
            try {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addString(0, key);
                row.addString(1, val);
                aTable.put(Arrays.toString(row.encodePrimaryKey()),key,val);
                OperationResponse rsInsert = session.apply(insert);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            // insert flush
            List<OperationResponse> orlist = session.flush();
            System.out.println("==========================="+orlist);
            for (OperationResponse or: orlist){
                if (or.hasRowError()){
                    Map<String,String> map = aTable.row(Arrays.toString(or.getRowError().getOperation().getRow().encodePrimaryKey()));
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        System.out.println("key :"+entry.getKey()+" value :"+entry.getValue());
                        Update update =table.newUpdate();
                        PartialRow urow = update.getRow();
                        urow.addString(0,entry.getKey());
                        urow.addString(1,entry.getValue());
                        session.apply(update);
                    }
                }
            }
            // update flush
            session.flush();
            aTable.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("---------------multiPut end--------------" +
                new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date()));
    }

    //
    private KuduClient kuduClient;
    private Options<T> options;
    private Serializer<T> serializer;
    private KuduSession session;
    private KuduTable table;

    public KuduState2(String hosts, Options<T> options, Serializer<T> serializer) {
        kuduClient = new KuduClient.KuduClientBuilder(hosts).build();
        this.options = options;
        this.serializer = serializer;
        this.session = kuduClient.newSession();
        this.session.setMutationBufferSpace(32*1024*1024);
        this.session.setTimeoutMillis(60*1000);
        this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
        try {
            table = kuduClient.openTable(options.tablename);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
