package com.github.ftrossbach.club_topicana.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by ftr on 10.11.17.
 */
public class ExpectedTopicConfiguration {

    private final String topicName;
    private final PartitionCount partitions;
    private final ReplicationFactor replicationFactor;
    private final Map<String, String> props;

    protected ExpectedTopicConfiguration(String topicName, PartitionCount partitions, ReplicationFactor replicationFactor, Map<String, String> props) {

        this.topicName = topicName;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.props = Collections.unmodifiableMap(props);
    }

    public String getTopicName() {
        return topicName;
    }

    public PartitionCount getPartitions() {
        return partitions;
    }

    public ReplicationFactor getReplicationFactor() {
        return replicationFactor;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public static class ExpectedTopicConfigurationBuilder {
        private final String topicName;
        private PartitionCount partitions = PartitionCount.ignore();
        private ReplicationFactor replicationFactor = ReplicationFactor.ignore();
        private Map<String, String> props = new HashMap<>();

        public ExpectedTopicConfigurationBuilder(String topicName) {
            this.topicName = topicName;
        }

        public ExpectedTopicConfigurationBuilder withPartitionCount(PartitionCount partitions) {
            this.partitions = partitions;
            return this;
        }

        public ExpectedTopicConfigurationBuilder withReplicationFactor(ReplicationFactor replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public ExpectedTopicConfigurationBuilder with(Map<String, String> props) {
            this.props.putAll(props);
            return this;
        }

        public ExpectedTopicConfigurationBuilder with(String key, String value) {
            this.props.put(key, value);
            return this;
        }


        public ExpectedTopicConfiguration build() {
            return new ExpectedTopicConfiguration(topicName, partitions, replicationFactor, props);
        }
    }
}
