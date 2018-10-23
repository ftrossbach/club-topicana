/**
 * Copyright © 2017 Florian Troßbach (trossbach@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ftrossbach.club_topicana.kafka_streams;

import com.github.ftrossbach.club_topicana.core.EmbeddedKafka;
import com.github.ftrossbach.club_topicana.core.ExpectedTopicConfiguration;
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaStreamsFactoryIntegrationTest {

    private static String bootstrapServers = null;
    private static EmbeddedKafka embeddedKafkaCluster = null;

    private static String testTopicName = "test_topic";

    @BeforeAll
    public static void initKafka() throws Exception {
        embeddedKafkaCluster = new EmbeddedKafka(1);
        embeddedKafkaCluster.start();
        bootstrapServers = embeddedKafkaCluster.bootstrapServers();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient ac = AdminClient.create(props)) {


            NewTopic testTopic = new NewTopic(testTopicName, 1, (short) 1);
            NewTopic output = new NewTopic("output", 1, (short) 1);

            Set<NewTopic> topics = new HashSet<>();
            topics.add(testTopic);
            topics.add(output);

            ac.createTopics(topics).all().get();

            KafkaProducer<String, String> producer = new KafkaProducer<>(Collections.singletonMap(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers), new StringSerializer(), new StringSerializer());
            producer.send(new ProducerRecord<>(testTopicName, "key", "value")).get();

        }
    }

    @Nested
    class Happy {

        @Test
        public void streams_with_props() throws Exception {
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder(testTopicName).build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


                        StreamsBuilder builder = new StreamsBuilder();
            String storeName = "testStore";
            builder.<String, String>table(testTopicName).groupBy((key, value) -> new KeyValue<>(key, value))
                    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.Long())
                            .withLoggingDisabled()
                            .withCachingDisabled());

            KafkaStreams streams = KafkaStreamsFactory.streams(builder.build(), props, Collections.singleton(expected));


            streams.start();

            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while (true) {

                    if (streams.state() == KafkaStreams.State.RUNNING) {
                        try {
                            ReadOnlyKeyValueStore<String, Long> store = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());

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
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder(testTopicName).build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


                        StreamsBuilder builder = new StreamsBuilder();
            String storeName = "testStore";
            builder.<String, String>table(testTopicName).groupBy((key, value) -> new KeyValue<>(key, value))
                    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.Long())
                            .withLoggingDisabled()
                            .withCachingDisabled());

            KafkaStreams streams = KafkaStreamsFactory.streams(builder.build(), props, Collections.singleton(expected));


            streams.start();

            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while (true) {

                    if (streams.state() == KafkaStreams.State.RUNNING) {
                        try {
                            ReadOnlyKeyValueStore<String, Long> store = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());

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
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder(testTopicName).build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


                        StreamsBuilder builder = new StreamsBuilder();
            String storeName = "testStore";
            builder.<String, String>table(testTopicName).groupBy((key, value) -> new KeyValue<>(key, value))
                    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.Long())
                            .withLoggingDisabled()
                            .withCachingDisabled());

            KafkaStreams streams = KafkaStreamsFactory.streams(builder.build(), props, new DefaultKafkaClientSupplier(), Collections.singleton(expected));


            streams.start();

            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while (true) {

                    if (streams.state() == KafkaStreams.State.RUNNING) {
                        try {
                            ReadOnlyKeyValueStore<String, Long> store = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());

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
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder(testTopicName).withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


                        StreamsBuilder builder = new StreamsBuilder();
            String storeName = "testStore";
            builder.<String, String>table(testTopicName).groupBy((key, value) -> new KeyValue<>(key, value))
                    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.Long())
                            .withLoggingDisabled()
                            .withCachingDisabled());

            assertThrows(MismatchedTopicConfigException.class, () -> KafkaStreamsFactory.streams(builder.build(), props, Collections.singleton(expected)));


        }


        @Test
        public void consumer_with_props_and_serializer() throws Exception {
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder(testTopicName).withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


                        StreamsBuilder builder = new StreamsBuilder();
            String storeName = "testStore";
            builder.<String, String>table(testTopicName).groupBy((key, value) -> new KeyValue<>(key, value))
                    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.Long())
                            .withLoggingDisabled()
                            .withCachingDisabled());

            assertThrows(MismatchedTopicConfigException.class, () -> KafkaStreamsFactory.streams(builder.build(), props, Collections.singleton(expected)));


        }

        @Test
        public void consumer_with_map_and_serializer() throws Exception {
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder(testTopicName).withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


                        StreamsBuilder builder = new StreamsBuilder();
            String storeName = "testStore";
            builder.<String, String>table(testTopicName).groupBy((key, value) -> new KeyValue<>(key, value))
                    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.Long())
                            .withLoggingDisabled()
                            .withCachingDisabled());

            assertThrows(MismatchedTopicConfigException.class, () -> KafkaStreamsFactory.streams(builder.build(), props, new DefaultKafkaClientSupplier(), Collections.singleton(expected)));

        }


    }


    @AfterAll
    public static void destroyKafka() {

        embeddedKafkaCluster.stop();
    }
}
