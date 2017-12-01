package com.github.ftrossbach.club_topicana.test;

import com.github.ftrossbach.club_topicana.core.EmbeddedKafka;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestApplication.class)
public class MatchingConfigIntegrationTest {

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

            List<NewTopic> topics = new ArrayList<>();
            topics.add(testTopic);
            topics.add(testTopic2);

            ac.createTopics(topics).all().get();


        }

        System.setProperty("club-topicana.bootstrap-servers", bootstrapServers);
        System.setProperty("club-topicana.fail-on-mismatch", "true");
    }


    @Test
    public void run() {


    }
}
