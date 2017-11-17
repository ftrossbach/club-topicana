package com.github.ftrossbach.club_topicana.core;

import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import java.util.Properties;

public class EmbeddedKafka extends EmbeddedKafkaCluster {
    public EmbeddedKafka(int numBrokers) {
        super(numBrokers);
    }

    public EmbeddedKafka(int numBrokers, Properties brokerConfig) {
        super(numBrokers, brokerConfig);
    }

    public EmbeddedKafka(int numBrokers, Properties brokerConfig, long mockTimeMillisStart) {
        super(numBrokers, brokerConfig, mockTimeMillisStart);
    }

    public EmbeddedKafka(int numBrokers, Properties brokerConfig, long mockTimeMillisStart, long mockTimeNanoStart) {
        super(numBrokers, brokerConfig, mockTimeMillisStart, mockTimeNanoStart);
    }


    public void stop(){
        after();
    }
}
