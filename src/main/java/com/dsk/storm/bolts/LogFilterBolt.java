package com.dsk.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.dsk.kudu.KudoManager;
import com.dsk.utils.Constants;
import com.dsk.utils.StringOperator;
import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Created by wanghaixing on 2015/11/12.
 */
public class LogFilterBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Connection conn = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        conn = KudoManager.getInstance().getConnection();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String line = tuple.getString(0);
            System.out.println(line);
            String[] items = line.split(",");
            if (items.length != 10) {
                System.out.println(line);
                return;
            }

            String uid = items[0];
            String sid = items[2];
            //primary key of tables
            String mid = StringOperator.encryptByMd5(uid + sid);

            String attrs = Joiner.on("\",\"").join(items);
            StringBuffer sb = new StringBuffer();
            sb.append("INSERT INTO ").append(Constants.UPUSERS_ATTR_TABLE).append(" VALUES (\"")
                    .append(mid).append("\",\"").append(attrs).append("\");");
            String insert_sql = sb.toString();
//            Connection conn = KudoManager.getInstance().getConnection();
            insert(conn, insert_sql, mid, items);

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

    public void insert(Connection conn, String sql, String mid, String[] items) {
        System.out.println(sql);
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
//            e.printStackTrace();
            String className = "";
            String methodName = "";
            for(StackTraceElement elem : e.getStackTrace()) {
                className = elem.getClassName();
                methodName = elem.getMethodName();
                if(className.equals(this.getClass().getName()) && methodName.equals("insert")) {
                    update(conn, mid, items);
                }
            }
        } finally {
            /*try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }*/
        }

    }

    public void update(Connection conn, String mid, String[] items) {
        StringBuffer sb = new StringBuffer();
        sb.append("UPDATE ").append(Constants.UPUSERS_ATTR_TABLE).append(" SET ")
                .append("ptid=\"").append(items[1]).append("\", n=\"").append(items[3])
                .append("\", ln=\"").append(items[4]).append("\", ver=\"").append(items[5])
                .append("\", pid=\"").append(items[6]).append("\", geoip_n=\"").append(items[7])
                .append("\", first_date=\"").append(items[8]).append("\", last_date=\"").append(items[9])
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

    public void cleanup() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
