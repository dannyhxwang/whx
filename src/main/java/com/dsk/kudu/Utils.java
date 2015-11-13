package com.dsk.kudu;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author yanbit
 * @date Nov 3, 2015 3:43:15 PM
 * @todo TODO
 */
public class Utils {

    private static final String CONNECTION_URL_PROPERTY = "connection.url";
    private static final String JDBC_DRIVER_NAME_PROPERTY =
            "jdbc.driver.class.name";

    public static String connectionUrl;
    public static String jdbcDriverName;

    static {
        try {
            loadConfiguration();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * format double to 0.00%
     *
     * @param number
     * @param newValue
     * @return
     */
    public static String formatPercent(double number, int newValue) {
        java.text.NumberFormat nf = java.text.NumberFormat.getPercentInstance();
        nf.setMinimumFractionDigits(newValue);
        return nf.format(number);
    }

    /**
     * load kudu properties
     *
     * @throws IOException
     */
    public static void loadConfiguration() throws IOException {
        InputStream input = null;
        try {
            String filename = "/KuduImpalaJDBC.conf";
            input = Utils.class.getResourceAsStream(filename);
            Properties prop = new Properties();
            prop.load(input);

            connectionUrl = prop.getProperty(CONNECTION_URL_PROPERTY);
            jdbcDriverName = prop.getProperty(JDBC_DRIVER_NAME_PROPERTY);
        } finally {
            try {
                if (input != null)
                    input.close();
            } catch (IOException e) {
            }
        }
    }

  /*public static void main(String[] args) {
      System.out.println(connectionUrl);
      System.out.println(jdbcDriverName);
  }*/
}