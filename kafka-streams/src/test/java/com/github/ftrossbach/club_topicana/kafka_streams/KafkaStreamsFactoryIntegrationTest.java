package com.github.ftrossbach.club_topicana.kafka_streams;

import com.github.ftrossbach.club_topicana.core.EmbeddedKafka;
import com.github.ftrossbach.club_topicana.core.ExpectedTopicConfiguration;
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;


import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

public class KafkaStreamsFactoryIntegrationTest {

    private static String bootstrapServers = null;
    private static EmbeddedKafka embeddedKafkaCluster = null;

    @BeforeAll
    public static void initKafka() throws Exception {
        embeddedKafkaCluster = new EmbeddedKafka(1);
        embeddedKafkaCluster.start();
        bootstrapServers = embeddedKafkaCluster.bootstrapServers();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient ac = AdminClient.create(props)) {


            NewTopic testTopic = new NewTopic("test_topic", 1, (short) 1);
            NewTopic output = new NewTopic("output", 1, (short) 1);

            Set<NewTopic> topics = new HashSet<>();
            topics.add(testTopic);
            topics.add(output);

            ac.createTopics(topics).all().get();

            KafkaProducer<String, String> producer = new KafkaProducer<>(Collections.singletonMap(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers), new StringSerializer(), new StringSerializer());
            producer.send(new ProducerRecord<>("test_topic", "key", "value")).get();

        }
    }

    @Nested
    class Happy {

        @Test
        public void streams_with_props() throws Exception {
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


            StreamsBuilder builder = new StreamsBuilder();
            builder.<String, String>table("test_topic").groupBy((key, value) -> new KeyValue<>(key, value)).count("store");

            KafkaStreams streams = KafkaStreamsFactory.streams(builder.build(), props, Collections.singleton(expected));


            streams.start();

            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while (true) {

                    if (streams.state() == KafkaStreams.State.RUNNING) {
                        try {
                            ReadOnlyKeyValueStore<String, Long> store = streams.store("store", QueryableStoreTypes.<String, Long>keyValueStore());

                            Long key = store.get("key");
                            if (key != null && key.equals(1L)) {
                                break;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    Thread.sleep(100);


                }
                return null;
            });


        }

        @Test
        public void streams_with_config() throws Exception {
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


            StreamsBuilder builder = new StreamsBuilder();
            builder.<String, String>table("test_topic").groupBy((key, value) -> new KeyValue<>(key, value)).count("store");

            KafkaStreams streams = KafkaStreamsFactory.streams(builder.build(), new StreamsConfig(props), Collections.singleton(expected));


            streams.start();

            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while (true) {

                    if (streams.state() == KafkaStreams.State.RUNNING) {
                        try {
                            ReadOnlyKeyValueStore<String, Long> store = streams.store("store", QueryableStoreTypes.<String, Long>keyValueStore());

                            Long key = store.get("key");
                            if (key != null && key.equals(1L)) {
                                break;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    Thread.sleep(100);


                }
                return null;
            });


        }

        @Test
        public void streams_with_config_and_supplier() throws Exception {
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


            StreamsBuilder builder = new StreamsBuilder();
            builder.<String, String>table("test_topic").groupBy((key, value) -> new KeyValue<>(key, value)).count("store");

            KafkaStreams streams = KafkaStreamsFactory.streams(builder.build(), new StreamsConfig(props), new DefaultKafkaClientSupplier(), Collections.singleton(expected));


            streams.start();

            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while (true) {

                    if (streams.state() == KafkaStreams.State.RUNNING) {
                        try {
                            ReadOnlyKeyValueStore<String, Long> store = streams.store("store", QueryableStoreTypes.<String, Long>keyValueStore());

                            Long key = store.get("key");
                            if (key != null && key.equals(1L)) {
                                break;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    Thread.sleep(100);


                }
                return null;
            });


        }


    }

    @Nested
    class Sad {

        @Test
        public void streams_with_props() throws Exception {
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


            StreamsBuilder builder = new StreamsBuilder();
            builder.<String, String>table("test_topic").groupBy((key, value) -> new KeyValue<>(key, value)).count("store");

            assertThrows(MismatchedTopicConfigException.class, () -> KafkaStreamsFactory.streams(builder.build(), props, Collections.singleton(expected)));


        }


        @Test
        public void consumer_with_props_and_serializer() throws Exception {
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


            StreamsBuilder builder = new StreamsBuilder();
            builder.<String, String>table("test_topic").groupBy((key, value) -> new KeyValue<>(key, value)).count("store");

            assertThrows(MismatchedTopicConfigException.class, () -> KafkaStreamsFactory.streams(builder.build(), new StreamsConfig(props), Collections.singleton(expected)));


        }

        @Test
        public void consumer_with_map_and_serializer() throws Exception {
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


            StreamsBuilder builder = new StreamsBuilder();
            builder.<String, String>table("test_topic").groupBy((key, value) -> new KeyValue<>(key, value)).count("store");

            assertThrows(MismatchedTopicConfigException.class, () -> KafkaStreamsFactory.streams(builder.build(), new StreamsConfig(props), new DefaultKafkaClientSupplier(), Collections.singleton(expected)));

        }


    }


    @AfterAll
    public static void destroyKafka() {

        embeddedKafkaCluster.stop();
    }
}
