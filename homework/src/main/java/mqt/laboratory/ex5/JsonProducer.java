package mqt.laboratory.ex5;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class JsonProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "jsonProducer";

    private static Producer<String, String> producer;

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);

        String json="{\n" +
                "    \"nume\":\"Iscruleasa\",\n" +
                "    \"prenume\":\"Sebastian\",\n" +
                "    \"varsta\":\"22 ani\",\n" +
                "    \"hobby\":\"fotbal\"\n" +
                "}";

        ProducerRecord<String, String> data1 = new ProducerRecord<>("topic1", json);
        try {
            producer.send(data1).get();
        } catch (InterruptedException | ExecutionException e){
            producer.flush();
        }
        producer.close();
    }
}
