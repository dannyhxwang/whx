package com.dsk.persist.hdfs;

import com.dsk.utils.Constants;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wanghaixing
 * on 2015/11/18 11:40.
 */
public class HdfsConsumer {
    private static Log LOG = LogFactory.getLog(HdfsConsumer.class);

    ConsumerConnector consumer;
    Map<String, List<KafkaStream<String, String>>> topicMessageStreams;

    public static void main(String[] args) {
        /*for(int i = 0; i < 10 ; i++) {
            LOG.info("hell0~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + "\n");
        }*/
        HdfsConsumer consumer = new HdfsConsumer();
        while(true) {
            consumer.consume();
        }
    }


    public HdfsConsumer() {
        Properties originalProps = new Properties();
        originalProps.put("zookeeper.connect", Constants.ZOOKEEPER_LIST);
        originalProps.put("group.id", Constants.GROUP_ID);
        originalProps.put("serializer.class", Constants.ENCODER);
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(originalProps));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(Constants.TOPIC_UPMATCH, 1);
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        topicMessageStreams = consumer.createMessageStreams(topicCountMap , keyDecoder, valueDecoder);

    }

    public void consume() {
        KafkaStream<String, String> kafkaStream = topicMessageStreams.get(Constants.TOPIC).get(0);
        ConsumerIterator<String, String> iterator = kafkaStream.iterator();
        while(iterator.hasNext()){
//            MessageAndMetadata<String, String> next = iterator.next();
//            System.out.println(next.message());
            String message = iterator.next().message();
            LOG.info(message + "\n");
        }
    }

    /*@Override
    public void run() {
        HdfsConsumer consumer = new HdfsConsumer();
        while(true) {
            consumer.consume();
        }
    }*/
}
