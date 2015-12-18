package com.dsk.storm.state;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.kududb.Schema;
import org.kududb.client.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * User: yanbit
 * Date: 2015/12/17
 * Time: 11:39
 */
public class KuduTest {
    private static KuduClient client;
    private static KuduSession session;

    static {
        client = new KuduClient.KuduClientBuilder("namenode").build();

        session = client.newSession();
        session.setMutationBufferSpace(32 * 1024 * 1024);
        session.setTimeoutMillis(60 * 1000);
        session.setFlushMode(KuduSession.FlushMode.MANUAL_FLUSH);
    }

    public static void main(String[] args) throws Exception {
        Table<String, String, String> aTable = HashBasedTable.create();
        String table = "my_first_table";
        KuduTable t = client.openTable(table);

//        Insert insert = t.newInsert();
//        PartialRow row = insert.getRow();
//        row.addLong(0, 1000000);
//        row.addString(1, "11111111111111111");
//        OperationResponse response = session.apply(insert);
//        List<OperationResponse> orlist = session.flush();

        Delete delete = t.newDelete();
        PartialRow drow = delete.getRow();
        drow.addLong(0, 100000);
        session.apply(delete);
        session.flush();
//        aTable.put(Arrays.toString(row.encodePrimaryKey()),"100000000","111111111111");
//        System.out.println(aTable);
//        //Row error for primary key=[-128, 0, 0, 0, 0, 15, 66, 64],
//        System.out.println("orlist"+orlist);
//        OperationResponse or = orlist.get(0);
//        System.out.println(Arrays.toString(row.encodePrimaryKey()));


//        System.out.println("!!!"+Arrays.toString(or.getRowError().getOperation().getRow().encodePrimaryKey()));
//        Update update = t.newUpdate();
//        PartialRow urow = update.getRow();
//        urow.addLong(0, 100000);
//        urow.addString(1, "22222222222222222");
//        session.apply(update);
//        session.flush();
        client.shutdown();
//        System.out.println(new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date()));
    }


    public void testUpdate() throws Exception {
        String table = "my_first_table";
        KuduClient client = new KuduClient.KuduClientBuilder("namenode").build();
        KuduTable t = client.openTable(table);
        Update update = t.newUpdate();
        PartialRow row = update.getRow();
        row.addLong(0, 10);
        row.addString(1, "11111111111111111");
        KuduSession session = client.newSession();
        session.apply(update);
        client.shutdown();
        System.out.println(new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date()));
    }

    public void testScan() throws Exception {
        String table = "my_first_table";
        List<String> cols = new ArrayList<String>();
        cols.add("name");
        KuduClient client = new KuduClient.KuduClientBuilder("namenode").build();
        KuduTable t = client.openTable(table);
        Schema s = t.getSchema();
        PartialRow start = s.newPartialRow();
        start.addLong("id", 10);
        PartialRow end = s.newPartialRow();
        end.addLong("id", 11);
        KuduScanner scanner = client.newScannerBuilder(t)
                .lowerBound(start)
                .exclusiveUpperBound(end)
                .setProjectedColumnNames(cols)
                .build();
        String v = null;
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                System.out.println("--------------");
                RowResult result = results.next();
                v = result.getString("name");
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
