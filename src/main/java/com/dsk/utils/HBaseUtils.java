package com.dsk.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by wanghaixing
 * on 2015/12/18 10:37.
 */
public class HBaseUtils {
    private static Configuration conf = null;
    private static Connection conn = null;

    public static synchronized Configuration getConfiguration() {
        if (conf == null) {
            conf = new Configuration();
            conf.addResource("hbase-site-cluster.xml");
            conf = HBaseConfiguration.create(conf);
        }
        return conf;
    }

    public static synchronized Connection getHConnection() {
        if (conn == null) {
            try {
                conn = ConnectionFactory.createConnection(getConfiguration());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

}
