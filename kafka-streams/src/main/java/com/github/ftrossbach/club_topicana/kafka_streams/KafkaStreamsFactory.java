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
package com.github.ftrossbach.club_topicana.kafka_streams;

import com.github.ftrossbach.club_topicana.core.ComparisonResult;
import com.github.ftrossbach.club_topicana.core.ExpectedTopicConfiguration;
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import com.github.ftrossbach.club_topicana.core.TopicComparer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import java.util.Collection;
import java.util.Properties;

public class KafkaStreamsFactory {

    public static KafkaStreams streams(Topology topology, Properties config, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration){
        return streams(topology, config, new DefaultKafkaClientSupplier(), expectedTopicConfiguration);
    }

    public static KafkaStreams streams(Topology topology, Properties config, KafkaClientSupplier clientSupplier, Collection<ExpectedTopicConfiguration> expectedTopicConfiguration){
        TopicComparer comparer = new TopicComparer(config);
        ComparisonResult result = comparer.compare(expectedTopicConfiguration);
        if (result.ok()) {
            return new KafkaStreams(topology, config, clientSupplier);
        } else {
            throw new MismatchedTopicConfigException(result);
        }
    }

}
