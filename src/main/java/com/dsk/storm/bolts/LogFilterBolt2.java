package com.dsk.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.dsk.utils.Constants;
import com.dsk.utils.StringOperator;
import org.apache.commons.lang.StringUtils;
import org.kududb.client.*;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by wanghaixing on 2015/11/12.
 */
public class LogFilterBolt2 extends BaseRichBolt {
    private OutputCollector collector;

    private KuduClient client;
    private KuduSession session;
    private KuduTable table_attr;
    private KuduTable table_days;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        client = new KuduClient.KuduClientBuilder(Constants.KUDU_MASTER).build();
        try {
            this.table_attr = client.openTable(Constants.UPUSERS_ATTR_TABLE);
            this.table_days = client.openTable(Constants.UPUSERS_DAYS_TABLE);
            this.session = client.newSession();
            this.session.setMutationBufferSpace(32*1024*1024);
            this.session.setTimeoutMillis(60*1000);
            this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        try {
            client.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void execute(Tuple tuple) {
        try {
            String line = tuple.getString(0);
            String[] items = line.split(",");
            if (items.length != 10) {
                System.out.println("=======================================ERROR LINE：" + line);
                this.collector.ack(tuple);
                return;
            }
            String uid = items[0];
            String sid = items[2];
            //primary key of tables
            String mid = StringOperator.encryptByMd5(uid + sid);
            insertUpdate(Constants.UPUSERS_ATTR_TABLE, mid, items);
            insert(Constants.UPUSERS_DAYS_TABLE, mid, Arrays.asList(items[9]).toArray(new String[1]));
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.ack(tuple);

//            this.collector.fail(tuple);
        }

    }

    /**
     * 执行插入更新操作
     * @param tablename
     * @param mid
     * @param fieldsValue
     */
    private void insertUpdate(String tablename, String mid, String[] fieldsValue) {

        checkSession();

        KuduTable table;
        if (Constants.UPUSERS_ATTR_TABLE.equals(tablename)) {
            table = table_attr;
        } else {
            table = table_days;
        }
        try {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            setInsertValue(mid, Constants.ATTR_INSERT_FIELDS, fieldsValue, row);
            OperationResponse rsInsert = session.apply(insert);
            if (rsInsert.hasRowError()) {
                if ("key already present".equals(rsInsert.getRowError().getMessage())) {
                    Update update = table.newUpdate();
                    PartialRow urow = update.getRow();
                    setUpdateValue(mid, Constants.ATTR_UPDATE_FIELDS, fieldsValue, urow);
                    OperationResponse rsUpdate = session.apply(update);
                    if (rsUpdate.hasRowError()) {
                        System.out.println("=======================================ERROR UPDATE :" + rsUpdate.getRowError());
                    } else {
                        System.out.println("=======================================UPDATE DATA:" + mid + ":" + Arrays.toString(fieldsValue));
                    }
                } else {
                    System.out.println("=======================================ERROR INSERT :" + rsInsert.getRowError());
                }
            } else {
                System.out.println("=======================================INSERT DATA:" + mid + ":" + Arrays.toString(fieldsValue));
            }
        } catch (Exception e) {
            collector.reportError(e);
        }
    }

    /**
     * 单纯插入
     * @param tablename
     * @param mid
     * @param fieldsValue
     */
    private void insert(String tablename, String mid, String[] fieldsValue) {

        checkSession();

        KuduTable table;
        if (Constants.UPUSERS_ATTR_TABLE.equals(tablename)) {
            table = table_attr;
        } else {
            table = table_days;
        }
        try {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            setInsertValue(mid, Constants.DAYS_FIELDS, fieldsValue, row);
            OperationResponse rsInsert = session.apply(insert);
            if (rsInsert.hasRowError()) {
                System.out.println("=======================================INSERT DATA:"+mid+"---------------"+
                        Arrays.toString(fieldsValue)+rsInsert.getRowError().getMessage());
            }
        } catch (Exception e) {
            collector.reportError(e);
        }
    }

    /**
     * 检查Session
     */
    private void checkSession() {
        if (session.isClosed()) {
            System.out.println("================================Session is closed ================================" + client);
            if (client == null) {
                client = new KuduClient.KuduClientBuilder(Constants.KUDU_MASTER).build();
            }
            if (StringUtils.isNotEmpty(client.toString())) {
                try {
                    table_attr = client.openTable(Constants.UPUSERS_ATTR_TABLE);
                    table_days = client.openTable(Constants.UPUSERS_DAYS_TABLE);
                    session = client.newSession();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("================================Session is created ================================" + client);
        }

    }

    /**
     * 设置Kudu Row
     * @param mid
     * @param fields
     * @param fieldsValue
     * @param row
     */
    private void setInsertValue(String mid, String[] fields, String[] fieldsValue, PartialRow row) {
        row.addString("mid", mid);
        for (int i = 0; i < fields.length; i++) {
            row.addString(fields[i], fieldsValue[i]);
        }
    }

    private void setUpdateValue(String mid, String[] fields, String[] fieldsValue, PartialRow row) {
        row.addString("mid", mid);
        int i = 0;
        for (; i < fields.length-1; i++) {
            row.addString(fields[i], fieldsValue[i]);
        }
        row.addString(fields[i], fieldsValue[i+1]);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
