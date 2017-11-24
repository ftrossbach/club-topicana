package com.github.ftrossbach.club_topicana.kafka_streams;

import com.github.ftrossbach.club_topicana.core.ComparisonResult;
import com.github.ftrossbach.club_topicana.core.ExpectedTopicConfiguration;
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import com.github.ftrossbach.club_topicana.core.TopicComparer;
import joptsimple.internal.Strings;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import java.util.Collection;
import java.util.Properties;

public class KafkaStreamsFactory {

    public static KafkaStreams streams(Topology topology, Properties props, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration){
        return streams(topology, new StreamsConfig(props), expectedTopicConfiguration);
    }

    public static KafkaStreams streams(Topology topology, StreamsConfig config, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration){
        return streams(topology, config, new DefaultKafkaClientSupplier(), expectedTopicConfiguration);
    }

    public static KafkaStreams streams(Topology topology, StreamsConfig config, KafkaClientSupplier clientSupplier, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration){
        TopicComparer comparer = new TopicComparer(Strings.join(config.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG), ","));
        ComparisonResult result = comparer.compare(expectedTopicConfiguration);
        if (result.ok()) {
            return new KafkaStreams(topology, config, clientSupplier);
        } else {
            throw new MismatchedTopicConfigException(result);
        }
    }

}
