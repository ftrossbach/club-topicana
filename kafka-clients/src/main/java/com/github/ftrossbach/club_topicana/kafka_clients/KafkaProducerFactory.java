package com.github.ftrossbach.club_topicana.kafka_clients;

import com.github.ftrossbach.club_topicana.core.ComparisonResult;
import com.github.ftrossbach.club_topicana.core.ExpectedTopicConfiguration;
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import com.github.ftrossbach.club_topicana.core.TopicComparer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class KafkaProducerFactory {


    public static  <K,V>  Producer<K,V> producer(Map<String, Object> configs, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) throws MismatchedTopicConfigException{
       return producer(configs, null, null, expectedTopicConfiguration);
    }

    public static <K,V> Producer<K,V> producer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) throws MismatchedTopicConfigException{
        Properties props = new Properties();
        props.putAll(configs);
        return producer(props, keySerializer, valueSerializer, expectedTopicConfiguration);
    }

    public static <K,V> Producer<K,V> producer(Properties properties, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) throws MismatchedTopicConfigException{
        return producer(properties, null, null, expectedTopicConfiguration);
    }

    public static <K,V>  Producer<K,V> producer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) throws MismatchedTopicConfigException{

        TopicComparer comparer = new TopicComparer(properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        ComparisonResult result = comparer.compare(expectedTopicConfiguration);
        if(result.ok()){
            return new KafkaProducer<>(properties, keySerializer, valueSerializer);
        } else {
            throw new MismatchedTopicConfigException(result);
        }


    }


}
