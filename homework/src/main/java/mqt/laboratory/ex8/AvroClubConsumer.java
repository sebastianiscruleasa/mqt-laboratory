package mqt.laboratory.ex8;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import mqt.laboratory.streams.avro.ClubAvro;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroClubConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(AvroClubConsumer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String OFFSET_RESET = "earliest";
    private static final String CONSUMER_GROUP_ID = "avroClubConsumer";

    private static KafkaConsumer<String, ClubAvro> kafkaConsumer;

    public AvroClubConsumer(Properties consumerPropsMap){
        kafkaConsumer = new KafkaConsumer<String, ClubAvro>(consumerPropsMap);
    }

    public void pollKafka(String kafkaTopicName){

        kafkaConsumer.subscribe(Collections.singleton(kafkaTopicName));

        Duration pollingTime = Duration.of(2, ChronoUnit.SECONDS);
        while (true){
            ConsumerRecords<String, ClubAvro> records = kafkaConsumer.poll(pollingTime);

            records.forEach(record -> {
                LOG.info("topic = {} value = {}", kafkaTopicName,  record.value());
            });
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url","http://localhost:8081");

        AvroClubConsumer consumer = new AvroClubConsumer(props);
        consumer.pollKafka("topic1");
    }
}
