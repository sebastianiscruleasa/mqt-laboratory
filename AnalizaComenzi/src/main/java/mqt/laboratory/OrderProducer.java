package mqt.laboratory;

import java.time.LocalDate;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import mqt.avro.Order;
import mqt.avro.Product;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class OrderProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "Producer";

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url","http://localhost:8081");

        KafkaProducer<String, Order> kafkaProducer = new KafkaProducer<String, Order>(props);

        String topic = "orders";


        Order order = Order.newBuilder().
                setOrderId(2).
                setClientId(1).
                setProducts(java.util.Arrays.asList(new Product(5, "prod5", (double)50), new Product(6, "prod6", (double)60))).
                setPlacedOrderDate(LocalDate.parse("2023-12-01")).
                setDeliveredOrderDate(LocalDate.parse("2023-12-17")).
                setTotalPrice((double)110).
                build();

        ProducerRecord<String, Order> data1 = new ProducerRecord<>(topic, String.valueOf(3),order);

        try {
            kafkaProducer.send(data1).get();
            System.out.println(data1);
        } catch (InterruptedException | ExecutionException e){
            kafkaProducer.flush();
        }
        kafkaProducer.close();
    }
}
