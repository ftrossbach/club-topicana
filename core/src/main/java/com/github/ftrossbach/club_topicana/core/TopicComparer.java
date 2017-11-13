package com.github.ftrossbach.club_topicana.core;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


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

        Map<String, ExpectedTopicConfiguration> expectedMap = expectedTopicConfiguration.stream().collect(toMap(ele -> ele.getTopicName(), ele -> ele));


        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        try (AdminClient adminClient = AdminClient.create(props)) {


            List<String> topicNames = expectedTopicConfiguration.stream()
                    .map((expectedTopic) -> expectedTopic.getTopicName()).collect(toList());


            Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(topicNames).values().entrySet().stream()
                    .flatMap(desc -> {
                        try {
                            TopicDescription topicDescription = desc.getValue().get();
                            return Collections.singletonList(topicDescription).stream();
                        } catch (Exception e) {
                            resultBuilder.addMissingTopic(desc.getKey());

                            return Collections.<TopicDescription>emptySet().stream();
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
                    resultBuilder.addMismatchingPartitionCount(exp.getTopicName(), exp.getReplicationFactor().count(), repflicationFactor);
                }

            });

            Map<String, Config> topicConfigs = adminClient.describeConfigs(topicNames.stream().map(this::topicNameToResource).collect(toList())).values().entrySet().stream()

                    .flatMap(tc -> {
                        Map<String, Config> res = new HashMap<>();
                        try {

                            res.put(tc.getKey().name(), tc.getValue().get());
                        } catch (InterruptedException | ExecutionException e) {
                            //TODO: don't ignore
                        }
                        return res.entrySet().stream();
                    })
                    .collect(toMap(i -> i.getKey(), i -> i.getValue()));


            expectedTopicConfiguration.stream().forEach(exp -> {
                Config config = topicConfigs.get(exp.getTopicName());

                exp.getProps().entrySet().forEach(prop -> {
                    ConfigEntry entry = config.get(prop.getKey());

                    if(entry == null) {
                        resultBuilder.addMismatchingConfiguration(exp.getTopicName(), prop.getValue(), prop.getValue(), null);
                    }

                    else if (!prop.getValue().equals(entry.value())) {
                        resultBuilder.addMismatchingConfiguration(exp.getTopicName(), prop.getValue(), prop.getValue(), entry.value());
                    }
                });
            });


            return resultBuilder.createComparisonResult();

        }


    }

    private ConfigResource topicNameToResource(String topicName) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    }
}
