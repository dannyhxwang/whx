package com.dsk.storm.function;


import backtype.storm.tuple.Values;
import com.dsk.utils.StringOperator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * User: yanbit
 * Date: 2015/12/21
 * Time: 17:31
 */
public class HBaseCountETL extends BaseFunction {


    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // etl
        String line = tuple.getString(0);
        String[] items = line.split(",");
        String rowkey = null;
        if (items.length==10){
            rowkey = StringOperator.encryptByMd5(items[0] + items[1] + items[2] + items[3] + items[4]);
            System.out.println("===========rowkey============= "+rowkey);
        }else{
            return;
        }
        collector.emit(new Values(rowkey));
    }
}
