package com.dsk.storm.test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * User: yanbit
 * Date: 2015/12/25
 * Time: 9:51
 */
public class TestProducer {
    public static void main(String[] args) {
        Random rnd = new Random();
        int events = 10000000;

        Properties props = new Properties();
        props.put("metadata.broker.list", "namenode:9092,datanode1:9092,datanode4:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        // ����producer
        Producer<String, String> producer = new Producer<String, String>(config);
        // ������������Ϣ
        long start = System.currentTimeMillis();
        for (long i = 0; i < events; i++) {
            String value = UUID.randomUUID().toString() + ",2,3,4,5,6,7,8,5,10";
            //���topic�����ڣ�����Զ�������Ĭ��replication-factorΪ1��partitionsΪ0
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                    "test_rk", null, value);
            producer.send(data);
            System.out.println(value);
        }
        System.out.println("��ʱ:" + (System.currentTimeMillis() - start));
        // �ر�producer
        producer.close();
    }
}
