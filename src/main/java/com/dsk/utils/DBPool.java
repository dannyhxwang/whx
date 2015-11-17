package com.dsk.utils;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author yanbit
 * @date Nov 17, 2015 1:20:13 PM
 * @todo TODO
 */
public class DBPool {
  private static DBPool dbPool;
  private ComboPooledDataSource dataSource;

  static {
    dbPool = new DBPool();
  }

  public DBPool() {
    try {
      dataSource = new ComboPooledDataSource();
      // dataSource.setUser("id");
      // dataSource.setPassword("pw");
      dataSource.setJdbcUrl("jdbc:hive2://10.1.3.59:21050/;auth=noSasl");
      dataSource.setDriverClass("org.apache.hive.jdbc.HiveDriver");
      dataSource.setInitialPoolSize(5);
      dataSource.setMinPoolSize(5);
      dataSource.setMaxPoolSize(10);
      dataSource.setMaxStatements(50);
      dataSource.setMaxIdleTime(60);
    } catch (PropertyVetoException e) {
      throw new RuntimeException(e);
    }
  }

  public final static DBPool getInstance() {
    return dbPool;
  }

  public final Connection getConnection() {
    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException("无法从数据源获取连接", e);
    }
  }

  public static void main(String[] args) throws SQLException {
    Connection con = null;

    try {
//      String sql1 =
//          "INSERT INTO upusers_attr_kudu_test2 VALUES (\"f3ea2b2c7777d265446b7b2928a650e9\",\"90e07a1c615674c059cdb7971e435cb9\",\"wzp_1217\",\"isafe\",\"us\",\"en\",\"5.6.123\",\"yacnvd\",\"in\",\"2015-01-17\",\"2015-01-17\")";
//      String sql2 =
//          "INSERT INTO upusers_attr_kudu_test2 VALUES (\"2f3ea2b2c7777d265446b7b2928a650e9\",\"90e07a1c615674c059cdb7971e435cb9\",\"wzp_1217\",\"isafe\",\"us\",\"en\",\"5.6.123\",\"yacnvd\",\"in\",\"2015-01-17\",\"2015-01-17\")";
      
      con = DBPool.getInstance().getConnection();
      Statement sts = con.createStatement();
      boolean b = sts.execute("INSERT IGNORE INTO my_first_table VALUES (99, \"sarah\"),(98,\"ssssssss\")");
      System.out.println(b);
      // sts.executeUpdate(
      // "UPDATE upusers_attr_kudu_test2 SET ptid=\"px_0625\", n=\"tw\",
      // ln=\"zh\", ver=\"6.3.82\", pid=\"yacnvd\", geoip_n=\"tw\",
      // first_date=\"2015-01-19\", last_date=\"2015-07-01\" where
      // mid=\"802a096635688187c6174ddab450d090\"");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (con != null)
        con.close();
    }
  }

}
