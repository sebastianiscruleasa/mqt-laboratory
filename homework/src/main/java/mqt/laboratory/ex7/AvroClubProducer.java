package mqt.laboratory.ex7;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import mqt.laboratory.streams.avro.ClubAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class AvroClubProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "avroClubProducer";

    private static Producer<String, ClubAvro> producer;

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url","http://localhost:8081");

        producer = new KafkaProducer<>(props);

        ClubAvro clubAvro = new ClubAvro("AVRO Borrusia Dortmund", "Germany", "Dortmund", 1909);

        ProducerRecord<String, ClubAvro> data1 = new ProducerRecord<>("topic1", clubAvro);
        try {
            producer.send(data1).get();
        } catch (InterruptedException | ExecutionException e){
            producer.flush();
        }
        producer.close();
    }
}
