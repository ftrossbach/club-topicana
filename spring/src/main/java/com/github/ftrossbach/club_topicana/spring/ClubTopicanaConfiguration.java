package com.github.ftrossbach.club_topicana.spring;

import com.github.ftrossbach.club_topicana.core.ComparisonResult;
import com.github.ftrossbach.club_topicana.core.ExpectedTopicConfiguration;
import com.github.ftrossbach.club_topicana.core.TopicComparer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.yaml.snakeyaml.Yaml;
import java.util.List;
import java.util.Map;


import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@Configuration
public class ClubTopicanaConfiguration {

    @Value("${club-topicana.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${club-topicana.config-file:club-topicana.yml}")
    private String configFile;

    @Bean
    public ComparisonResult clubTopicanaComparisonResult(){

        List<Map<String, Object>> map = (List<Map<String, Object>>) new Yaml().load(this.getClass().getClassLoader().getResourceAsStream(configFile));


        List<ExpectedTopicConfiguration> expectedConfig = map.stream().map(entry -> {
            ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder build = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder((String) entry.get("name"));

            if (entry.get("replication-factor") != null) {
                build.withReplicationFactor((Integer) entry.get("replication-factor"));
            }

            if (entry.get("partition-count") != null) {
                build.withReplicationFactor((Integer) entry.get("partition-count"));
            }

            if (entry.get("config") != null) {
                List<Map<String, Object>> config = (List<Map<String, Object>>) entry.get("config");

                config.stream().forEach(configMap -> {
                    Map<String, String> stringifiedConfig = configMap.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));
                    build.withConfig(stringifiedConfig);
                });

            }


            return build.build();

        }).collect(toList());


        ComparisonResult result = new TopicComparer(bootstrapServers).compare(expectedConfig);
        return result;
    }


    @Bean
    public ComparisonResultEvaluator clubTopicanaEvaluator(){
        return new ComparisonResultEvaluator(clubTopicanaComparisonResult());
    }


}
