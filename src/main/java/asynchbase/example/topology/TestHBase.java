/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package asynchbase.example.topology;

import asynchbase.example.spout.RandomKeyValueBatchSpout;
import asynchbase.trident.state.AsyncHBaseMapState;
import asynchbase.utils.serializer.AsyncHBaseSerializer;
import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.TransactionalValue;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * This topology use Trident sums up the random values emited by the spout for each key.<br/>
 * And then persisit it to HBase using AsyncHBaseMapState.<br/>
 * A DRPC query the state every second.<br/>
 * </p>
 * <p>
 * You'll have to set hBaseConfig and to tweak parallel hints and config
 * to match your configuration.
 * </p>
 */
public class TestHBase {
    public static final Logger log = LoggerFactory.getLogger(TestHBase.class);

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static StormTopology buildTopology(LocalDRPC drpc) {

        // 序列化的谁？
        class AsyncHBaseLongSerializer implements AsyncHBaseSerializer {
            @Override
            public byte[] serialize(Object value) {
                return Long.toString((long) value).getBytes();
            }

            @Override
            public void prepare(Map conf) {

            }
        }

        // 生成随机数据
        class MyRandomKeyValueBatchSpout extends RandomKeyValueBatchSpout {
            @Override
            public Object nextValue() {
                return generator.nextInt(10);
            }
        }


        // topology设置
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("stream", new MyRandomKeyValueBatchSpout().setKeys(new Values("foo", "bar", "baz")));

        // hbase config
        AsyncHBaseMapState.Options sumStateOptions = new AsyncHBaseMapState.Options<TransactionalValue>();
        sumStateOptions.cluster = "hbase-cluster";
        sumStateOptions.table = "test";
        sumStateOptions.columnFamily = "f";
        sumStateOptions.columnQualifier = "count";

        TridentState sumState = stream
                .groupBy(new Fields("key"))
                .persistentAggregate(AsyncHBaseMapState.transactional(sumStateOptions), new Fields("value"), new Sum(), new Fields("sum")).parallelismHint(5);

        sumState
                .newValuesStream()
                .each(new Fields("key", "sum"), new Debug());

//        topology.newDRPCStream("sum-drpc", drpc)
//            .stateQuery(sumState, new Fields("args"), new MapGet(), new Fields("sum"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);

        Map<String, String> hBaseConfig = new HashMap<String, String>();
        hBaseConfig.put("zkQuorum", "datanode1");
        conf.put("hbase-cluster", hBaseConfig);
        conf.setNumWorkers(1);
        StormSubmitter.submitTopology("test_hbase", conf, buildTopology(null));
    }
}