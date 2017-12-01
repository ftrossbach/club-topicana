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

import com.github.ftrossbach.club_topicana.core.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaProducerFactoryIntegrationTest {

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
        }
    }


    @Nested
    class Happy{

        @Test
        public void producer_with_props() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            Producer<Object, Object> producer = KafkaProducerFactory.producer(props, Collections.singleton(expected));

            Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<Object, Object>("test_topic", null, null));
            producer.flush();
            metadataFuture.get(5, TimeUnit.SECONDS);

        }


        @Test
        public void producer_with_props_and_serializer() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            Producer<String, String> producer = KafkaProducerFactory.producer(props, new StringSerializer(), new StringSerializer(), Collections.singleton(expected));

            Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<>("test_topic", null, null));
            producer.flush();
            metadataFuture.get(5, TimeUnit.SECONDS);

        }

        @Test
        public void producer_with_map_and_serializer() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();


            Producer<String, String> producer = KafkaProducerFactory.producer(Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers), new StringSerializer(), new StringSerializer(), Collections.singleton(expected));

            Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<>("test_topic", null, null));
            producer.flush();
            metadataFuture.get(5, TimeUnit.SECONDS);

        }

        @Test
        public void producer_with_map() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            Map<String, Object> props = new HashMap<>();


            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            Producer<String, String> producer = KafkaProducerFactory.producer(props, Collections.singleton(expected));

            Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<>("test_topic", null, null));
            producer.flush();
            metadataFuture.get(5, TimeUnit.SECONDS);

        }


    }

    @Nested
    class Sad{

        @Test
        public void producer_with_props() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            assertThrows(MismatchedTopicConfigException.class, () -> KafkaProducerFactory.producer(props, Collections.singleton(expected)) );


        }


        @Test
        public void producer_with_props_and_serializer() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            assertThrows(MismatchedTopicConfigException.class, () -> KafkaProducerFactory.producer(props, new StringSerializer(), new StringSerializer(), Collections.singleton(expected)) );


        }

        @Test
        public void producer_with_map_and_serializer() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();


            assertThrows(MismatchedTopicConfigException.class, () -> KafkaProducerFactory.producer(Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers), new StringSerializer(), new StringSerializer(), Collections.singleton(expected)));
        }

        @Test
        public void producer_with_map() throws Exception{
            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            Map<String, Object> props = new HashMap<>();


            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            assertThrows(MismatchedTopicConfigException.class, () -> KafkaProducerFactory.producer(props, Collections.singleton(expected)));


        }


    }



    @AfterAll
    public static void destroyKafka(){

        embeddedKafkaCluster.stop();
    }
}
