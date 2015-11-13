package com.dsk.kudu;

import java.sql.*;

/**
 * Created by wanghaixing on 2015/11/12.
 */
public class KudoManager {

    private volatile static KudoManager kudu_instance = null;

    private KudoManager() {};

    public synchronized static KudoManager getInstance() {
        if(kudu_instance == null) {
            kudu_instance = new KudoManager();
        }
        return kudu_instance;
    }

    public Connection getConnection() {
        Connection conn = null;

        try {
            Class.forName(Utils.jdbcDriverName);
            conn = DriverManager.getConnection(Utils.connectionUrl);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }



}