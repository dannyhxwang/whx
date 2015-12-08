package com.dsk.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.dsk.utils.Constants;
import com.dsk.utils.DBPool;
import com.dsk.utils.StringOperator;
import org.kududb.client.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Created by wanghaixing on 2015/11/12.
 */
public class LogFilterBolt2 extends BaseRichBolt {
    private OutputCollector collector;
    private Connection conn = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
//        conn = KudoManager.getInstance().getConnection();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String line = tuple.getString(0);
            System.out.println(line);
            String[] items = line.split(",");
            if (items.length != 10) {
                System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$error line："+line);
                return;
            }

            String uid = items[0];
            String sid = items[2];
            //primary key of tables
            String mid = StringOperator.encryptByMd5(uid + sid);

            insertAttrTable(mid, items);
            //insertDaysTable(mid, items[9]);

            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.ack(tuple);

//            this.collector.fail(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void insertAttrTable(String mid, String[] items) {
        String KUDU_MASTER = System.getProperty(
                "kuduMaster", "namenode");
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        // TODO insert
        String tablename ="upusers_attr_kudu_test_api";
        String[] fields = {"uid","ptid","sid","n","ln","ver","pid","geoip_n","first_date","last_date"};
        try {
            KuduTable table = client.openTable(tablename);
            KuduSession session = client.newSession();
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            setOpValue(mid, items, fields, row);
            OperationResponse rsInsert = session.apply(insert);
            // TODO update
            if ("key already present".equals(rsInsert.getRowError().getMessage())){
                System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$key already present : update");
                Update update = table.newUpdate();
                PartialRow urow = update.getRow();
                setOpValue(mid,items,fields,urow);
                OperationResponse rsUpdate = session.apply(update);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void setOpValue(String mid, String[] items, String[] fields, PartialRow row) {
        row.addString("mid",mid);
        for (int i = 0; i < fields.length; i++) {
            row.addString(fields[i],items[i]);
        }
    }

    public void insertDaysTable(String mid, String day) {
        conn = DBPool.getInstance().getConnection();
        StringBuffer sb = new StringBuffer();
        sb.append("INSERT IGNORE INTO ").append(Constants.UPUSERS_DAYS_TABLE).append(" VALUES (\"")
                .append(mid).append("\",\"").append(day).append("\");");
        String insert_sql = sb.toString();
        System.out.println(insert_sql);

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(insert_sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void updateAttrs(Connection conn, String mid, String[] items) {
        StringBuffer sb = new StringBuffer();
        sb.append("UPDATE ").append(Constants.UPUSERS_ATTR_TABLE).append(" SET ")
                .append("ptid=\"").append(items[1]).append("\", n=\"").append(items[3])
                .append("\", ln=\"").append(items[4]).append("\", ver=\"").append(items[5])
                .append("\", pid=\"").append(items[6]).append("\", geoip_n=\"").append(items[7])
                .append("\", last_date=\"").append(items[9])
                .append("\" where mid=\"").append(mid).append("\";");
        String update_sql = sb.toString();
        System.out.println(update_sql);

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(update_sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /*public void cleanup() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
}
