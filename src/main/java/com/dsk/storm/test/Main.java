package com.dsk.storm.test;

import java.util.HashSet;
import java.util.Set;

/**
 * User: yanbit
 * Date: 2015/12/24
 * Time: 9:56
 */
public class Main {
    public static void main(String[] args) {
//        TridentConfig config = new TridentConfig("testa", "key");
//        config.addColumn("f", "count");
//
//        Map<String, Set<String>> map = config.columnFamilies;
//        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
//            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
//        }
        Set<String> cols = new HashSet<String>();
        cols.add("ab");
        cols.add("aa");
        cols.add("bb");

        System.out.println(cols.toString());

    }
}
