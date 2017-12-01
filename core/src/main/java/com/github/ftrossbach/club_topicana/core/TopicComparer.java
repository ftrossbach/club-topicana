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

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import java.util.*;
import java.util.concurrent.ExecutionException;


import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Created by ftr on 10.11.17.
 */
public class TopicComparer {

    private final String bootstrapServer;

    public TopicComparer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }


    public ComparisonResult compare(Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) {

        ComparisonResult.ComparisonResultBuilder resultBuilder = new ComparisonResult.ComparisonResultBuilder();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        try (AdminClient adminClient = AdminClient.create(props)) {


            List<String> topicNames = expectedTopicConfiguration.stream()
                    .map((expectedTopic) -> expectedTopic.getTopicName()).collect(toList());

            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);


            Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(topicNames).values().entrySet().stream()
                    .flatMap(desc -> {
                        try {
                            TopicDescription topicDescription = desc.getValue().get();
                            return Collections.singletonList(topicDescription).stream();
                        } catch (ExecutionException e) {
                            resultBuilder.addMissingTopic(desc.getKey());

                            return Collections.<TopicDescription>emptySet().stream();
                        } catch (InterruptedException e) {
                            throw new EvaluationException("Exception during adminClient.describeTopics", e);
                        }
                    }).collect(toMap(i -> i.name(), i -> i));




            expectedTopicConfiguration.stream()

                    .filter(exp -> topicDescriptions.containsKey(exp.getTopicName()))
                    .forEach(exp -> {
                TopicDescription topicDescription = topicDescriptions.get(exp.getTopicName());

                if (exp.getPartitions().isSpecified() && topicDescription.partitions().size() != exp.getPartitions().count()) {
                    resultBuilder.addMismatchingPartitionCount(exp.getTopicName(), exp.getPartitions().count(), topicDescription.partitions().size());
                }
                int repflicationFactor = topicDescription.partitions().stream().findFirst().get().replicas().size();
                if (exp.getReplicationFactor().isSpecified() && repflicationFactor != exp.getReplicationFactor().count()) {
                    resultBuilder.addMismatchingReplicationFactor(exp.getTopicName(), exp.getReplicationFactor().count(), repflicationFactor);
                }

            });

            DescribeConfigsResult configs = adminClient.describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, "topic_name")));

            Map<String, Config> topicConfigs = adminClient.describeConfigs(topicNames.stream().map(this::topicNameToResource).collect(toList())).values().entrySet().stream()

                    .flatMap(tc -> {
                        Map<String, Config> res = new HashMap<>();
                        try {

                            res.put(tc.getKey().name(), tc.getValue().get());
                        } catch (InterruptedException | ExecutionException e) {
                            throw new EvaluationException("Exception during adminClient.describeConfigs", e);
                        }
                        return res.entrySet().stream();
                    })
                    .collect(toMap(i -> i.getKey(), i -> i.getValue()));


            expectedTopicConfiguration.stream().forEach(exp -> {
                Config config = topicConfigs.get(exp.getTopicName());

                exp.getProps().entrySet().forEach(prop -> {
                    ConfigEntry entry = config.get(prop.getKey());

                    if(entry == null) {
                        resultBuilder.addMismatchingConfiguration(exp.getTopicName(), prop.getKey(), prop.getValue(), null);
                    }

                    else if (!prop.getValue().equals(entry.value())) {
                        resultBuilder.addMismatchingConfiguration(exp.getTopicName(), prop.getKey(), prop.getValue(), entry.value());
                    }
                });
            });


            return resultBuilder.build();

        }


    }

    private ConfigResource topicNameToResource(String topicName) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    }
}
