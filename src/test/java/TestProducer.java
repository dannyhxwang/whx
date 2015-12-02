import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.CharSet;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by user on 8/4/14.
 */
public class TestProducer {
    final static String TOPIC = "test_upusers_2";

    public static void main(String[] argv) {
        while (true) {
            sendUpUserMessage();
        }
    }

    /**
     *
     */
    public static void sendUpUserMessage() {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "10.1.3.56:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer =
                new kafka.javaapi.producer.Producer<String, String>(producerConfig);

        // ------------------------------------------------------------------------------------
        List<String> lines = null;
        try {
            lines = Files.readLines(
                    new File("D:\\work\\whx\\src\\test\\resources\\upusers.csv"),
                    Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (String line : lines) {
            ArrayList<String> list = new ArrayList<String>(
                    Arrays.asList(line.replace(";NULL", "").split(",")));
//            System.out.println(list);
            if (list.size() != 0) {
                String mid = list.remove(0);
                list.remove(0);
                String nline = Hashing.md5().hashString(
                        UUID.randomUUID().toString() + new Random().nextLong(),
                        Charsets.UTF_8) + ","
                        + Joiner.on(",").join(list.toArray()).toString();
                KeyedMessage<String, String> message =
                        new KeyedMessage<String, String>(TOPIC, nline);
                producer.send(message);

            }
        }
        // producer.close();

    }
}