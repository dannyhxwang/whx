package com.dsk.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.dsk.bean.UpMatcher;
import com.dsk.hbase.HbaseTask;
import com.dsk.utils.StringOperator;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wanghaixing
 * on 2015/12/17 11:06.
 */
public class UpMatchBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, UpMatcher> dataMap;
    private String preDate;
    private int num = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        dataMap = new HashMap<String, UpMatcher>();
    }

    //uid,db,tab,rec,ext,ver,nation,pid,count,date
    //XXXXX,stu_t,TAB_STARTUP_APP,aWRtYW4kaWRtYW4uZXhl,RGlzYWJsZQ==,6.7.69,us,isafe,1,20151216
    @Override
    public void execute(Tuple tuple) {
        byte[] byteField = tuple.getBinaryByField("bytes");
        String line = new String(byteField);
//        String line = tuple.getString(0);
        String items[] = line.split(",");
        if(items.length == 10) {
            System.out.println(line);
            String currentDate = items[9].trim();

            //如果当前数据日期与上一条数据日期不同，则认为前一天的数据发送完毕
//            if (!currentDate.equals(preDate)) {
//            String sourceComponent = tuple.getSourceComponent();
//            if(sourceComponent.equals(backtype.storm.Constants.SYSTEM_COMPONENT_ID)) {
            if (num > 20000) {
                //store to hbase
                if (dataMap.size() > 0) {
                    new HbaseTask(dataMap).dowork();
                    num = 0;
                }
            } else {
                String rowkey = StringOperator.encryptByMd5(items[0] + items[1] + items[2] + items[3] + items[4]);
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
                num++;
            }
//            preDate = currentDate;

        }
        this.collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }


}
