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

@Configuration
public class ClubTopicanaConfiguration {

    @Value("${club-topicana.bootstrap-servers}")
    private String bootstrapServers;


    @Bean
    public ComparisonResult result(){

        List<Map<String, Object>> map = (List<Map<String, Object>>) new Yaml().load(this.getClass().getClassLoader().getResourceAsStream("club-topicana.yml"));


        map.stream().map(entry -> {
            ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder build = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder((String) entry.get("name"));

            if(entry.get("replication-factor") != null){
                build.withReplicationFactor(Integer.valueOf((String) entry.get("replication-factor")));
            }

            if(entry.get("broker-count") != null){
                build.withReplicationFactor(Integer.valueOf((String) entry.get("broker-count")));
            }


            return null;

        });


        System.out.println(map);
        new TopicComparer(bootstrapServers).compare(null);
        return null;
    }


}
