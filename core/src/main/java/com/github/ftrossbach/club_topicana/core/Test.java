package com.github.ftrossbach.club_topicana.core;

import java.util.Collections;

/**
 * Created by ftr on 10.11.17.
 */
public class Test {

    public static void main(String[] args) {
        TopicComparer topicComparer = new TopicComparer("localhost:9092");

        ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder ac = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("ac")
                .withPartitionCount(PartitionCount.of(5))
                .withReplicationFactor(ReplicationFactor.of(4))
                .with("hurz", "urz");

        ComparisonResult result = topicComparer.compare(Collections.singleton(ac.build()));

        System.out.println(result);
    }
}
