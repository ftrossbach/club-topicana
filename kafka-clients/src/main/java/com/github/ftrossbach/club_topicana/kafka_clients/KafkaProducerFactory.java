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
