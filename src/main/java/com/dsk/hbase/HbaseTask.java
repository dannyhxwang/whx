package com.dsk.hbase;

import com.dsk.bean.UpMatcher;
import com.dsk.utils.Constants;
import com.dsk.utils.HBaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wanghaixing
 * on 2015/12/17 17:03.
 */
public class HbaseTask {
    private Map<String, UpMatcher> dataMap;
    private List<Put> upList = new ArrayList<Put>();
    private Connection conn = HBaseUtils.getHConnection();

    public HbaseTask(Map<String, UpMatcher> dataMap) {
        this.dataMap = dataMap;
    }

    public void dowork() {
        UpMatcher upMatcher = null;
        ExecutorService service = Executors.newFixedThreadPool(2);

        for(Map.Entry<String, UpMatcher> entry : dataMap.entrySet()) {
            if (upList.size() >= Constants.HBASE_BATCH_SIZE) {
                service.submit(new LoadToHBase(upList));
                upList.clear();
            } else {
                upMatcher = entry.getValue();
                Put put = new Put(entry.getKey().getBytes());
                put.addColumn(Constants.FAMILY_NAME.getBytes(), Bytes.toBytes("uid"), Bytes.toBytes(upMatcher.getUid()));
                put.addColumn(Constants.FAMILY_NAME.getBytes(), Bytes.toBytes("db"), Bytes.toBytes(upMatcher.getDb()));
                put.addColumn(Constants.FAMILY_NAME.getBytes(), Bytes.toBytes("tab"), Bytes.toBytes(upMatcher.getTab()));
                put.addColumn(Constants.FAMILY_NAME.getBytes(), Bytes.toBytes("rec"), Bytes.toBytes(upMatcher.getRec()));
                put.addColumn(Constants.FAMILY_NAME.getBytes(), Bytes.toBytes("ext"), Bytes.toBytes(upMatcher.getExt()));
                put.addColumn(Constants.FAMILY_NAME.getBytes(), Bytes.toBytes("ver"), Bytes.toBytes(upMatcher.getVer()));
                put.addColumn(Constants.FAMILY_NAME.getBytes(), Bytes.toBytes("nation"), Bytes.toBytes(upMatcher.getNation()));
                put.addColumn(Constants.FAMILY_NAME.getBytes(), Bytes.toBytes("pid"), Bytes.toBytes(upMatcher.getPid()));
                put.addColumn(Constants.FAMILY_NAME.getBytes(), Bytes.toBytes("count"), Bytes.toBytes(upMatcher.getCount()));
                upList.add(put);
            }
        }
        if (upList.size() > 0) {
            service.submit(new LoadToHBase(upList));
        }
        service.shutdown();
    }

    class LoadToHBase implements Runnable {
        private List<Put> puts;

        public LoadToHBase(List<Put> puts) {
            this.puts = puts;
        }

        @Override
        public void run() {
            Table table = null;
            try {
                table = conn.getTable(TableName.valueOf(Constants.UPMATCH_TABLE));
                table.put(puts);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
