/**
 * Copyright © 2017 Florian Troßbach (trossbach@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ftrossbach.club_topicana.core;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;


/**
 * Created by ftr on 10.11.17.
 */
public class TopicComparer {

    private final Properties adminClientConfig;


    public TopicComparer(Properties adminClientConfig) {
        this.adminClientConfig = adminClientConfig;
    }


    public ComparisonResult compare(Collection<ExpectedTopicConfiguration> expectedTopicConfiguration) {

        final ComparisonResult.ComparisonResultBuilder resultBuilder = new ComparisonResult.ComparisonResultBuilder();

        try (AdminClient adminClient = AdminClient.create(adminClientConfig)) {

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
                            resultBuilder.addMismatchingPartitionCount(exp.getTopicName(), exp.getPartitions().count(),
                                    topicDescription.partitions().size());
                        }
                        int replicationFactor = topicDescription.partitions().stream().findFirst().get().replicas().size();
                        if (exp.getReplicationFactor().isSpecified() && replicationFactor != exp.getReplicationFactor().count()) {
                            resultBuilder.addMismatchingReplicationFactor(exp.getTopicName(), exp.getReplicationFactor().count(),
                                    replicationFactor);
                        }

                    });

            DescribeConfigsResult configs = adminClient.describeConfigs(
                    Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, "topic_name")));

            Map<String, Optional<Config>> topicConfigs = adminClient.describeConfigs(
                    topicNames.stream().map(this::topicNameToResource).collect(toList())).values().entrySet().stream()

                    .flatMap(tc -> {
                        Map<String, Optional<Config>> res = new HashMap<>();
                        try {
                            res.put(tc.getKey().name(), Optional.of(tc.getValue().get()));
                        } catch (InterruptedException | ExecutionException e) {
                            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                                res.put(tc.getKey().name(), Optional.empty());
                            } else {
                                throw new EvaluationException("Exception during adminClient.describeConfigs", e);
                            }
                        }
                        return res.entrySet().stream();
                    })
                    .collect(toMap(i -> i.getKey(), i -> i.getValue()));


            expectedTopicConfiguration.stream().forEach(exp -> {
                Optional<Config> optionalConfig = topicConfigs.get(exp.getTopicName());

                exp.getProps().entrySet().forEach(prop -> {
                    ConfigEntry entry = optionalConfig.map(config -> config.get(prop.getKey())).orElse(null);

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
