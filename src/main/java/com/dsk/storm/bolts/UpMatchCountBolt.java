package com.dsk.storm.bolts;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.dsk.bean.UpMatcher;
import com.dsk.hbase.HbaseTask;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wanghaixing
 * on 2015/12/23 11:42.
 */
public class UpMatchCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, UpMatcher> dataMap;
    private Map stormConf;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.stormConf = stormConf;
        dataMap = new HashMap<String, UpMatcher>();
    }

    @Override
    public void execute(Tuple input) {
//        String rowkey = input.getStringByField("rowkey");
        Object obj = input.getValue(0);
        String rowkey = obj.toString();
//        System.out.println(rowkey);
//        if (rowkey.equals(String.valueOf(input.getSourceTask()))) {
        if (rowkey.equals(String.valueOf(stormConf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS)))) {
//            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + stormConf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS));
            System.out.println("----------------------------" + dataMap.size());
            new HbaseTask(dataMap).dowork();
        } else {
//            String line = input.getStringByField("line");
            try {
                if (input.size() == 2) {
                    Object obj2 = input.getValue(1);
                    String line = obj2.toString();
                    String[] items = line.split(",");
                    UpMatcher upMatcher = dataMap.get(rowkey);
                    int count = 1;
                    if (StringUtils.isNotBlank(items[8])) {
                        count = Integer.valueOf(items[8]);
                    }
                    if (upMatcher != null) {
                        count += upMatcher.getCount();
                        upMatcher.setCount(count);
                    } else {
                        upMatcher = new UpMatcher(items[0], items[1], items[2], items[3], items[4], items[5],
                                items[6], items[7], count);
                    }
                    dataMap.put(rowkey, upMatcher);
                } else {
                    System.out.println("++++++++++++++++++++++++++" + input.size() + "++++++++++++++++++++++" + rowkey);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("&&&&&&&&&&&&&&&&&&&&&&"  + input.size());
                System.out.println("==================================" + input);
            }

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
