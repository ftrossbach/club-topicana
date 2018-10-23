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
package com.github.ftrossbach.club_topicana.kafka_clients;

import com.github.ftrossbach.club_topicana.core.EmbeddedKafka;
import com.github.ftrossbach.club_topicana.core.ExpectedTopicConfiguration;
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

public class KafkaConsumerFactoryIntegrationTest {

    private static String bootstrapServers = null;
    private static EmbeddedKafka embeddedKafkaCluster = null;




    @BeforeAll
    public static void initKafka() throws Exception{
        embeddedKafkaCluster = new EmbeddedKafka(1);
        embeddedKafkaCluster.start();
        bootstrapServers = embeddedKafkaCluster.bootstrapServers();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try(AdminClient ac = AdminClient.create(props)){


            NewTopic testTopic = new NewTopic("test_topic", 1, (short) 1);



            ac.createTopics(Collections.singleton(testTopic)).all().get();

            KafkaProducer<String, String> producer = new KafkaProducer<>(Collections.singletonMap(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers), new StringSerializer(), new StringSerializer());
            producer.send(new ProducerRecord<>("test_topic", "value")).get();

        }
    }


    @Nested
    class Happy{

        @Test
        public void consumer_with_props() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            Consumer<String, String> consumer = KafkaConsumerFactory.consumer(props, Collections.singleton(expected));

            consumer.subscribe(Collections.singleton("test_topic"));

            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if(! records.isEmpty()) {
                        break;
                    }

                }
                return null;
            });





        }


        @Test
        public void consumer_with_props_and_serializer() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            Consumer<String, String> consumer = KafkaConsumerFactory.consumer(props, new StringDeserializer(), new StringDeserializer(), Collections.singleton(expected));

            consumer.subscribe(Collections.singleton("test_topic"));
            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if(! records.isEmpty()) {
                        break;
                    }

                }
                return null;
            });

        }

        @Test
        public void consumer_with_map_and_serializer() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();


            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            Consumer<String, String> consumer = KafkaConsumerFactory.consumer(props, new StringDeserializer(), new StringDeserializer(), Collections.singleton(expected));

            consumer.subscribe(Collections.singleton("test_topic"));
            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if(! records.isEmpty()) {
                        break;
                    }

                }
                return null;
            });

        }

        @Test
        public void consumer_with_map() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            Consumer<String, String> consumer = KafkaConsumerFactory.consumer(props, new StringDeserializer(), new StringDeserializer(), Collections.singleton(expected));

            consumer.subscribe(Collections.singleton("test_topic"));
            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {

                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if(! records.isEmpty()) {
                        break;
                    }

                }
                return null;
            });

        }


    }

    @Nested
    class Sad{

        @Test
        public void consumer_with_props() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            assertThrows(MismatchedTopicConfigException.class, () -> KafkaConsumerFactory.consumer(props, Collections.singleton(expected)) );


        }


        @Test
        public void consumer_with_props_and_serializer() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            assertThrows(MismatchedTopicConfigException.class, () -> KafkaConsumerFactory.consumer(props, new StringDeserializer(), new StringDeserializer(), Collections.singleton(expected)) );


        }

        @Test
        public void consumer_with_map_and_serializer() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();


            assertThrows(MismatchedTopicConfigException.class, () -> KafkaConsumerFactory.consumer(Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers), new StringDeserializer(), new StringDeserializer(), Collections.singleton(expected)));
        }

        @Test
        public void consumer_with_map() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            Map<String, Object> props = new HashMap<>();


            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            assertThrows(MismatchedTopicConfigException.class, () -> KafkaConsumerFactory.consumer(props, Collections.singleton(expected)));


        }


    }



    @AfterAll
    public static void destroyKafka(){

        embeddedKafkaCluster.stop();
    }
}
