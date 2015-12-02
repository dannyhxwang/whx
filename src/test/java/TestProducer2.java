import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadFactory;

/**
 * Created by user on 8/4/14.
 */
public class TestProducer2 {
//    final static String TOPIC = "test_whx_1";
    final static String TOPIC = "test_upusers";

    public static void main(String[] argv) throws Exception {

        TestProducer2 t = new TestProducer2();
//        while (true) {
            t.sendUpUserMessage();
//        }
    }

    public void sendUpUserMessage() throws IOException, InterruptedException {
        Properties properties = new Properties();
//        properties.put("metadata.broker.list", "10.1.3.55:9092,10.1.3.56:9092,10.1.3.59:9092");
        properties.put("metadata.broker.list", "106.39.79.80:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);

        InputStream in = this.getClass().getResourceAsStream("/upusers1.csv");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = reader.readLine()) != null) {
//            System.out.println(line);
            ArrayList<String> list = new ArrayList<String>(Arrays.asList(line.replace(";NULL", "").split(",")));
            if (list.size() != 0) {
                list.remove(0);
                String uid = list.remove(0);
                String nline = Hashing.md5().hashString(uid + System.currentTimeMillis() + new Random().nextLong(), Charsets.UTF_8)
                        + "," + Joiner.on(",").join(list.toArray()).toString();
//                String nline = Joiner.on(",").join(list.toArray()).toString();
                KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, nline);
                Thread.sleep(1000);
                producer.send(message);
//                System.out.println(nline);
            }
        }
    }

    public void sendUpUserMessage2() throws IOException {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "106.39.79.80:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);

        KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, "hssssssssssssssssssssssssssssss");
        producer.send(message);

    }
}