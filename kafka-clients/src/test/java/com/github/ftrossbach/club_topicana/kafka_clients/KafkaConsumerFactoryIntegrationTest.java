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
import java.util.*;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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
                    ConsumerRecords<String, String> records = consumer.poll(100);
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
                    ConsumerRecords<String, String> records = consumer.poll(100);
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
                    ConsumerRecords<String, String> records = consumer.poll(100);
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
                    ConsumerRecords<String, String> records = consumer.poll(100);
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
        public void producer_with_map() throws Exception{
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
