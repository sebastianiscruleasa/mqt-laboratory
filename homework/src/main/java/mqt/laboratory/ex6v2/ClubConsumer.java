package mqt.laboratory.ex6v2;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import mqt.laboratory.ex5v2.Club;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClubConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(ClubConsumer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String OFFSET_RESET = "earliest";
    private static final String CONSUMER_GROUP_ID = "clubConsumer";

    private static KafkaConsumer<String, Club> kafkaConsumer;

    public ClubConsumer(Properties consumerPropsMap){
        kafkaConsumer = new KafkaConsumer<String, Club>(consumerPropsMap);
    }

    public void pollKafka(String kafkaTopicName){

        kafkaConsumer.subscribe(Collections.singleton(kafkaTopicName));

        Duration pollingTime = Duration.of(2, ChronoUnit.SECONDS);
        while (true){
            ConsumerRecords<String, Club> records = kafkaConsumer.poll(pollingTime);

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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ClubConsumer consumer = new ClubConsumer(props);
        consumer.pollKafka("topic1");
    }
}
