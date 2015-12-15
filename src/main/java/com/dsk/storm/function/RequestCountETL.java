package com.dsk.storm.function;


import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * User: yanbit
 * Date: 2015/12/15
 * Time: 11:43
 */
public class RequestCountETL extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // TODO need ETL ?
        collector.emit(new Values(tuple.getString(0)));
    }
}
