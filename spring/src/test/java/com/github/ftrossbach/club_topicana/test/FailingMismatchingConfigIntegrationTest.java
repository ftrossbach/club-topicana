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
package com.github.ftrossbach.club_topicana.test;

import com.github.ftrossbach.club_topicana.core.EmbeddedKafka;
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.*;


import static junit.framework.TestCase.fail;

@RunWith(SpringRunner.class)
public class FailingMismatchingConfigIntegrationTest {

    private static String bootstrapServers = null;
    private static EmbeddedKafka embeddedKafkaCluster = null;

    @BeforeClass
    public static void initKafka() throws Exception {
        embeddedKafkaCluster = new EmbeddedKafka(1);
        embeddedKafkaCluster.start();
        bootstrapServers = embeddedKafkaCluster.bootstrapServers();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient ac = AdminClient.create(props)) {

            NewTopic testTopic = new NewTopic("test_topic", 1, (short) 1);
            NewTopic testTopic2 = new NewTopic("test_topic2", 1, (short) 1);


            Map<String, String> config = new HashMap<>();
            config.put("cleanup.policy", "compact");
            testTopic2.configs(config);

            List<NewTopic> topics = new ArrayList<>();
            topics.add(testTopic);
            topics.add(testTopic2);

            ac.createTopics(topics).all().get();


        }

        System.setProperty("club-topicana.bootstrap-servers", bootstrapServers);
        System.setProperty("club-topicana.fail-on-mismatch", "true");
        System.setProperty("club-topicana.config-file", "club-topicana-mismatch.yml");
    }


    @Test(timeout = 30_000)
    public void fail_because_of_topic_mismatch() {
        try{
            TestApplication.main(new String[]{});
            fail();
        } catch (Exception e){
            Throwable cause = null;
            while(true){
                if(e.getCause() == null){
                    fail();
                } else {
                    cause = e.getCause();
                    if(cause instanceof MismatchedTopicConfigException){
                        break;
                    }
                }
            }
        }


    }
}
