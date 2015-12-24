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
        if (tuple.getString(0).split(",").length == 10) {
            String[] lines = tuple.getString(0).split(",");
            for (String value : lines) {
                collector.emit(new Values(value));
            }
        } else {
            System.out.println("ERROR=== " + tuple.getString(0));
        }
    }
}
