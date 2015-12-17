package com.dsk.storm.state;

import org.kududb.Schema;
import org.kududb.client.*;

import java.util.ArrayList;
import java.util.List;

/**
 * User: yanbit
 * Date: 2015/12/17
 * Time: 11:39
 */
public class KuduTest {
    public static void main(String[] args) throws Exception {
        String table = "my_first_table";
        List<String> cols = new ArrayList<String>();
        cols.add("value");
        KuduClient client = new KuduClient.KuduClientBuilder("namenode").build();
        KuduTable t = client.openTable(table);
        Schema s = t.getSchema();
        PartialRow start  = s.newPartialRow();
        start.addLong("key",0);
        PartialRow end = s.newPartialRow();
        end.addLong("key",1);
        KuduScanner scanner = client.newScannerBuilder(t)
                .lowerBound(start)
                .exclusiveUpperBound(end)
                .setProjectedColumnNames(cols)
                .build();
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                System.out.println(result.getString("value"));
            }
        }

        client.shutdown();
    }
}
