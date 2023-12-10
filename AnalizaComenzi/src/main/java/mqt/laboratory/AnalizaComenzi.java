package mqt.laboratory;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import mqt.avro.Order;
import mqt.avro.OrderDeliveryTime;
import mqt.avro.Product;
import mqt.avro.ProductCount;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;

import static java.util.Collections.singletonMap;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

public class AnalizaComenzi {
    private static int CLIENT_ID = 1;

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "analiza-comenzi");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        final String schemaRegistryUrl = "http://localhost:8081";
        KafkaStreams streams = new KafkaStreams(buildTopology(singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)), config);

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (final Exception e) {
                // ignored
            }
        }));
    }

    static Topology buildTopology(final Map<String, String> serdeConfig) {

        final Serde<Order> valueOrderSerde = new SpecificAvroSerde<>();
        valueOrderSerde.configure(serdeConfig, false);

        final Serde<Product> valueProductSerde = new SpecificAvroSerde<>();
        valueProductSerde.configure(serdeConfig, false);

        final Serde<OrderDeliveryTime> valueOrderDeliveryTimeSerde = new SpecificAvroSerde<>();
        valueOrderDeliveryTimeSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Order> orders = builder.stream("orders", Consumed.with(Serdes.String(), valueOrderSerde));

        // toate comenzile pentru clientul cu id-ul CLIENT_ID
        KStream<String, Order> clientOrders = orders.filter((key, order) -> order.getClientId() == CLIENT_ID);

        // afisarea comenzilor pentru clientul cu id-ul CLIENT_ID
        clientOrders.foreach((key, value) -> {
            System.out.println("Key: " + key + ", Value: " + value);
        });

        // afisarea celei mai lungi si celei mai scurte comenzi a clientului cu id-ul CLIENT_ID
        final KStream<String, OrderDeliveryTime> orderDeliveryTimeStream = clientOrders.mapValues((key, order) -> new OrderDeliveryTime(order.getOrderId(), order.getDeliveredOrderDate().toEpochDay() - order.getPlacedOrderDate().toEpochDay()));

        final LongestOrShortestOrderSerde longestOrShortestOrderSerde = new LongestOrShortestOrderSerde();

        orderDeliveryTimeStream.groupBy((key, orderDeliveryTime) -> KeyValue.pair("all", orderDeliveryTime))
                .aggregate(LongestAndShortestOrder::new,
                        (aggKey, value, aggregate) -> {
                            aggregate.computeLongestShortestOrder(value);
                            return aggregate;
                        },
                        Materialized.<String, LongestAndShortestOrder, KeyValueStore<Bytes, byte[]>>as("longest-shortest-order")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(longestOrShortestOrderSerde)
                );
        final KStream<String, LongestAndShortestOrder> longestAndShortestOrder = builder.stream("longest-shortest-order", Consumed.with(Serdes.String(), longestOrShortestOrderSerde));
        longestAndShortestOrder.foreach((key, value) -> {
            System.out.println("Cea mai lunga comanda: " + value.getLongestOrder());
            System.out.println("Cea mai scurta comanda: " + value.getShortestOrder());
        });

        final SpecificAvroSerde<ProductCount> productCountSerde = new SpecificAvroSerde<>();
        productCountSerde.configure(serdeConfig, false);
//        toate produsele comandate de clientul cu id-ul CLIENT_ID

        final KTable<Integer, Long> productCountTable = clientOrders.flatMapValues(Order::getProducts).
                groupBy((key,value) -> value.getProductId())
                .count();


        final TopFiveProductsSerde topFiveProductsSerde = new TopFiveProductsSerde();

        productCountTable.groupBy((productId, count) ->
                                KeyValue.pair("all",
                                        new ProductCount((long) productId, count)),
                        Grouped.with(Serdes.String(), productCountSerde))
                .aggregate(TopFiveProducts::new,
                        (aggKey, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        (aggKey, value, aggregate) -> {
                            aggregate.remove(value);
                            return aggregate;
                        },
                        Materialized.<String, TopFiveProducts, KeyValueStore<Bytes, byte[]>>as("top-five-products")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(topFiveProductsSerde)
                );

        final KStream<String, TopFiveProducts> topFiveProducts = builder.stream("top-five-products", Consumed.with(Serdes.String(), topFiveProductsSerde));
        topFiveProducts.foreach((key, value) -> {
            System.out.println("Top 5 produse: " + value);
        });


        return builder.build();
    }

    static class TopFiveProducts implements Iterable<ProductCount> {
        private final Map<Long, ProductCount> currentProducts = new HashMap<>();
        private final TreeSet<ProductCount> topFiveProducts = new TreeSet<>((o1, o2) -> {
            final Long o1Count = o1.getCount();
            final Long o2Count = o2.getCount();

            final int result = o2Count.compareTo(o1Count);
            if (result != 0) {
                return result;
            }
            final Long o1ProductId = o1.getProductId();
            final Long o2ProductId = o2.getProductId();
            return o1ProductId.compareTo(o2ProductId);
        });

        @Override
        public String toString() {
            return currentProducts.toString();
        }

        public void add(final ProductCount productCount) {
            if(currentProducts.containsKey(productCount.getProductId())) {
                topFiveProducts.remove(currentProducts.remove(productCount.getProductId()));
            }
            topFiveProducts.add(productCount);
            currentProducts.put(productCount.getProductId(), productCount);
            if (topFiveProducts.size() > 5) {
                final ProductCount last = topFiveProducts.last();
                currentProducts.remove(last.getProductId());
                topFiveProducts.remove(last);
            }
        }

        void remove(final ProductCount value) {
            topFiveProducts.remove(value);
            currentProducts.remove(value.getProductId());
        }


        @Override
        public Iterator<ProductCount> iterator() {
            return topFiveProducts.iterator();
        }
    }


    static class LongestAndShortestOrder {
        private OrderDeliveryTime longestOrder = null;
        private OrderDeliveryTime shortestOrder = null;

        public void computeLongestShortestOrder(OrderDeliveryTime orderDeliveryTime) {
            if(longestOrder == null) {
                longestOrder = copyOrderDeliveryTime(orderDeliveryTime);
            } else if(longestOrder.getDeliveryTime() < orderDeliveryTime.getDeliveryTime()){
                longestOrder = copyOrderDeliveryTime(orderDeliveryTime);
            }
            if(shortestOrder == null) {
                shortestOrder = orderDeliveryTime;
            } else if(shortestOrder.getDeliveryTime() > orderDeliveryTime.getDeliveryTime()){
                shortestOrder = copyOrderDeliveryTime(orderDeliveryTime);
            }
        }

        private static OrderDeliveryTime copyOrderDeliveryTime(OrderDeliveryTime orderDeliveryTime) {
            return OrderDeliveryTime.newBuilder().
                    setOrderId(orderDeliveryTime.getOrderId()).
                    setDeliveryTime(orderDeliveryTime.getDeliveryTime()).
                    build();
        }

        public OrderDeliveryTime getLongestOrder() {
            return longestOrder;
        }

        public OrderDeliveryTime getShortestOrder() {
            return shortestOrder;
        }
    }

    private static class LongestOrShortestOrderSerde implements Serde<LongestAndShortestOrder> {

        @Override
        public void configure(final Map<String, ?> map, final boolean b) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<LongestAndShortestOrder> serializer() {
            return new Serializer<LongestAndShortestOrder>() {
                @Override
                public void configure(final Map<String, ?> map, final boolean b) {
                }

                @Override
                public byte[] serialize(final String s, final LongestAndShortestOrder longestAndShortestOrder) {
                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    final DataOutputStream
                            dataOutputStream =
                            new DataOutputStream(out);
                    try {
                        dataOutputStream.writeInt(longestAndShortestOrder.getLongestOrder().getOrderId());
                        dataOutputStream.writeLong(longestAndShortestOrder.getLongestOrder().getDeliveryTime());
                        dataOutputStream.writeInt(longestAndShortestOrder.getShortestOrder().getOrderId());
                        dataOutputStream.writeLong(longestAndShortestOrder.getShortestOrder().getDeliveryTime());
                        dataOutputStream.flush();
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                    return out.toByteArray();
                }

                @Override
                public void close() {

                }
            };
        }

        @Override
        public Deserializer<LongestAndShortestOrder> deserializer() {
            return new Deserializer<LongestAndShortestOrder>() {
                @Override
                public void configure(final Map<String, ?> map, final boolean b) {

                }

                @Override
                public LongestAndShortestOrder deserialize(final String s, final byte[] bytes) {
                    if (bytes == null || bytes.length == 0) {
                        return null;
                    }
                    final LongestAndShortestOrder result = new LongestAndShortestOrder();

                    final DataInputStream
                            dataInputStream =
                            new DataInputStream(new ByteArrayInputStream(bytes));

                    try {
                        while(dataInputStream.available() > 0) {
                            result.computeLongestShortestOrder(new OrderDeliveryTime(dataInputStream.readInt(),
                                    dataInputStream.readLong()));
                        }
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                    return result;
                }

                @Override
                public void close() {

                }
            };
        }
    }

    private static class TopFiveProductsSerde implements Serde<TopFiveProducts> {

        @Override
        public void configure(final Map<String, ?> map, final boolean b) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<TopFiveProducts> serializer() {
            return new Serializer<TopFiveProducts>() {
                @Override
                public void configure(final Map<String, ?> map, final boolean b) {
                }

                @Override
                public byte[] serialize(final String s, final TopFiveProducts topFiveProducts) {
                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    final DataOutputStream
                            dataOutputStream =
                            new DataOutputStream(out);
                    try {
                        for (final ProductCount productCount : topFiveProducts) {
                            dataOutputStream.writeLong(productCount.getProductId());
                            dataOutputStream.writeLong(productCount.getCount());
                        }
                        dataOutputStream.flush();
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                    return out.toByteArray();
                }

                @Override
                public void close() {

                }
            };
        }

        @Override
        public Deserializer<TopFiveProducts> deserializer() {
            return new Deserializer<TopFiveProducts>() {
                @Override
                public void configure(final Map<String, ?> map, final boolean b) {

                }

                @Override
                public TopFiveProducts deserialize(final String s, final byte[] bytes) {
                    if (bytes == null || bytes.length == 0) {
                        return null;
                    }
                    final TopFiveProducts result = new TopFiveProducts();

                    final DataInputStream
                            dataInputStream =
                            new DataInputStream(new ByteArrayInputStream(bytes));

                    try {
                        while(dataInputStream.available() > 0) {
                            result.add(new ProductCount(dataInputStream.readLong(),
                                    dataInputStream.readLong()));
                        }
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                    return result;
                }

                @Override
                public void close() {

                }
            };
        }
    }

}
