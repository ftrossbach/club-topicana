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
