package com.dsk.storm.test;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * User: yanbit
 * Date: 2015/12/23
 * Time: 10:30
 */
public class TestHBaseETL extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // TODO need etl ?
        System.out.println("--------------------------------------" + tuple.toString());
        if (tuple.getString(0).split(",").length == 10) {
            String[] lines = tuple.getString(0).split(",");
            if (lines[8].length() == 0) {
                lines[8] = "0";
            }
            collector.emit(new Values(lines[0], lines[1], lines[2], lines[3], lines[4], lines[5], lines[6], lines[7], Long.valueOf(lines[8]), lines[9]));
        }
    }
}
