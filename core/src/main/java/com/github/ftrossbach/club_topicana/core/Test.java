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
