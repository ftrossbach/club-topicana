package com.github.ftrossbach.club_topicana.kafka_clients;

import com.github.ftrossbach.club_topicana.core.ComparisonResult;
import com.github.ftrossbach.club_topicana.core.ExpectedTopicConfiguration;
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import com.github.ftrossbach.club_topicana.core.TopicComparer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerFactory {


    public static <K, V> Consumer<K, V> consumer(Map<String, Object> configs, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) throws MismatchedTopicConfigException {
        return consumer(configs, null, null, expectedTopicConfiguration);
    }

    public static <K, V> Consumer<K, V> consumer(Map<String, Object> configs, Deserializer<K> keySerializer, Deserializer<V> valueSerializer, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) throws MismatchedTopicConfigException {
        Properties props = new Properties();
        props.putAll(configs);
        return consumer(props, keySerializer, valueSerializer, expectedTopicConfiguration);
    }

    public static <K, V> Consumer<K, V> consumer(Properties properties, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) throws MismatchedTopicConfigException {
        return consumer(properties, null, null, expectedTopicConfiguration);
    }

    public static <K, V> Consumer<K, V> consumer(Properties properties, Deserializer<K> keySerializer, Deserializer<V> valueSerializer, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) throws MismatchedTopicConfigException {

        TopicComparer comparer = new TopicComparer(properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        ComparisonResult result = comparer.compare(expectedTopicConfiguration);
        if (result.ok()) {
            return new KafkaConsumer<>(properties, keySerializer, valueSerializer);
        } else {
            throw new MismatchedTopicConfigException(result);
        }


    }


}
