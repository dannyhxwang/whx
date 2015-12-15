package com.dsk.storm.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * User: yanbit
 * Date: 2015/12/15
 * Time: 11:38
 */
public class PrintFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        System.out.println("##########################print##########################"+tuple);
    }
}
