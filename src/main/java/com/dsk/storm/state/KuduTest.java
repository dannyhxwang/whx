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
        KuduClient client = new KuduClient.KuduClientBuilder("namenode").build();
        KuduTable t = client.openTable(table);
        Update update = t.newUpdate();
        PartialRow row = update.getRow();
        row.addLong(0,10);
        row.addString(1,"11111111111111111");
        KuduSession session=client.newSession();
        session.apply(update);
        client.shutdown();
    }

    public void  test() throws Exception {
        String table = "my_first_table";
        List<String> cols = new ArrayList<String>();
        cols.add("name");
        KuduClient client = new KuduClient.KuduClientBuilder("namenode").build();
        KuduTable t = client.openTable(table);
        Schema s = t.getSchema();
        PartialRow start  = s.newPartialRow();
        start.addLong("id", 10);
        PartialRow end = s.newPartialRow();
        end.addLong("id",11);
        KuduScanner scanner = client.newScannerBuilder(t)
                .lowerBound(start)
                .exclusiveUpperBound(end)
                .setProjectedColumnNames(cols)
                .build();
        String v =null;
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                System.out.println("--------------");
                RowResult result = results.next();
                v=result.getString("name");
//                if (Strings.isNotEmpty(result.getString("name"))){
//                    v=result.getString("name");
//                }else {
//                    System.out.println("empty");
//                }
            }
        }
        System.out.println(v);


        client.shutdown();
    }
}
