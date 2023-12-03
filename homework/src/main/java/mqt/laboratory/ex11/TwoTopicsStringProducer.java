package mqt.laboratory.ex11;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class TwoTopicsStringProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "twoTopicsStringProducer";

    private static Producer<String, String> producer;

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> dataTopic1 = new ProducerRecord<>("topic1stream", "Testing Topic 1");

        ProducerRecord<String, String> dataTopic2 = new ProducerRecord<>("topic2stream",  "Testing Topic 2");
        try {
            producer.send(dataTopic1).get();
            producer.send(dataTopic2).get();
        } catch (InterruptedException | ExecutionException e){
            producer.flush();
        }
        producer.close();
    }
}
