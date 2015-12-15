package com.dsk.utils;

/**
 * Created by wanghaixing on 2015/11/12.
 */
public class Constants {
    //zookeeper
    public static final String ZOOKEEPER_LIST = "datanode1:2181,datanode2:2181,datanode4:2181";

    //kudu table
    public static final String KUDU_MASTER = "namenode";
    public static final String UPUSERS_ATTR_TABLE = "upusers_attr_kudu_test";
    public static final String UPUSERS_DAYS_TABLE = "upusers_days_kudu_test";

    public static final String[] ATTR_INSERT_FIELDS = {"uid", "ptid", "sid", "n", "ln", "ver", "pid", "geoip_n", "first_date", "last_date"};
    public static final String[] ATTR_UPDATE_FIELDS = {"uid", "ptid", "sid", "n", "ln", "ver", "pid", "geoip_n", "last_date"};
    public static final String[] DAYS_FIELDS= {"day"};

    //storm
    public static final String TOPIC = "test_upusers";
    public static final String TOPIC_REQUEST_COUNT = "test_request_count";
//    public static final String TOPIC = "test";
    public static final String GROUP_ID = "TEST";

    //kafka
    public static final String ENCODER = "kafka.serializer.StringEncoder";
    public static final String BROKER_LIST = "namenode:9092,datanode1:9092,datanode4:9092";


}