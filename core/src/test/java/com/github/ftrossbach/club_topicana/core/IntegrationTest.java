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
package com.github.ftrossbach.club_topicana.core;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntegrationTest {

    private static String bootstrapServers = null;
    private static EmbeddedKafka embeddedKafkaCluster = null;

    private TopicComparer unitUnderTest;
    private static Properties props;


    @BeforeAll
    public static void initKafka() throws Exception{
        embeddedKafkaCluster = new EmbeddedKafka(1);
        embeddedKafkaCluster.start();
        bootstrapServers = embeddedKafkaCluster.bootstrapServers();

        props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try(AdminClient ac = AdminClient.create(props)){


            NewTopic testTopic = new NewTopic("test_topic", 1, (short) 1);



            ac.createTopics(Collections.singleton(testTopic)).all().get();
        }
    }

    @BeforeEach
    public void setUp(){
        unitUnderTest = new TopicComparer(props);
    }

    @Nested
    class Existence{
        @Test
        public void topic_exists(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertTrue(result.ok());


        }

        @Test
        public void topic_not_exists(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("nonexisting_topic").build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertFalse(result.ok());

        }
    }

    @Nested
    class ReplicationFactor{
        @Test
        public void rf_fits(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(1).build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertTrue(result.ok());


        }

        @Test
        public void rf_fits_not(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withReplicationFactor(2).build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertAll(() -> assertFalse(result.ok()),
                    () -> assertThat(result.getMismatchingReplicationFactor().get("test_topic").getExpectedValue(), is(equalTo(2))),
                    () -> assertThat(result.getMismatchingReplicationFactor().get("test_topic").getActualValue(), is(equalTo(1))));


        }


    }

    @Nested
    class PartitionCount{
        @Test
        public void count_fits(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withPartitionCount(1).build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertTrue(result.ok());


        }

        @Test
        public void count_fits_not(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withPartitionCount(2).build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertAll(() -> assertFalse(result.ok()),
                    () -> assertThat(result.getMismatchingPartitionCount().get("test_topic").getExpectedValue(), is(equalTo(2))),
                    () -> assertThat(result.getMismatchingPartitionCount().get("test_topic").getActualValue(), is(equalTo(1))));


        }


    }

    @Nested
    class Configuration{
        @Test
        public void single_config_fits(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withConfig("cleanup.policy", "delete").build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertTrue(result.ok());


        }

        @Test
        public void multi_config_fits(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withConfig("compression.type", "producer").withConfig("cleanup.policy", "delete").build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertTrue(result.ok());


        }

        @Test
        public void multi_config_fits_not(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withConfig("compression.type", "gzip").withConfig("cleanup.policy", "compact").build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertAll(() -> assertFalse(result.ok()),
                    () -> assertThat(result.getMismatchingConfiguration().get("test_topic").size(), is(equalTo(2))),
                    () -> assertThat(result.getMismatchingConfiguration().get("test_topic").stream().filter(comp -> comp.getProperty().equals("cleanup.policy")).findFirst().get().getExpectedValue(), is(equalTo("compact"))),
                    () -> assertThat(result.getMismatchingConfiguration().get("test_topic").stream().filter(comp -> comp.getProperty().equals("cleanup.policy")).findFirst().get().getActualValue(), is(equalTo("delete"))),
                    () -> assertThat(result.getMismatchingConfiguration().get("test_topic").stream().filter(comp -> comp.getProperty().equals("compression.type")).findFirst().get().getExpectedValue(), is(equalTo("gzip"))),
                    () -> assertThat(result.getMismatchingConfiguration().get("test_topic").stream().filter(comp -> comp.getProperty().equals("compression.type")).findFirst().get().getActualValue(), is(equalTo("producer")))

            );


        }

        @Test
        public void unknown_config(){

            ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").withConfig("unknown", "config").build();

            ComparisonResult result = unitUnderTest.compare(Collections.singleton(expected));

            assertAll(() -> assertFalse(result.ok()),
                    () -> assertThat(result.getMismatchingConfiguration().get("test_topic").size(), is(equalTo(1))),
                    () -> assertThat(result.getMismatchingConfiguration().get("test_topic").stream().findFirst().get().getExpectedValue(), is(equalTo("config"))),
                    () -> assertThat(result.getMismatchingConfiguration().get("test_topic").stream().findFirst().get().getActualValue(), is(equalTo(null)))

            );


        }


    }







    @AfterAll
    public static void destroyKafka(){

        embeddedKafkaCluster.stop();
    }


}
