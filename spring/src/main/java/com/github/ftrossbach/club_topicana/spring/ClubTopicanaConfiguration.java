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
package com.github.ftrossbach.club_topicana.spring;

import com.github.ftrossbach.club_topicana.core.ComparisonResult;
import com.github.ftrossbach.club_topicana.core.ConfigParser;
import com.github.ftrossbach.club_topicana.core.ExpectedTopicConfiguration;
import com.github.ftrossbach.club_topicana.core.TopicComparer;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.Properties;

@Configuration
public class ClubTopicanaConfiguration {

    private final ConfigParser configParser = new ConfigParser();
    @Value("${club-topicana.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${club-topicana.config-file:club-topicana.yml}")
    private String configFile;

    @Bean
    public ComparisonResult clubTopicanaComparisonResult(){

        Collection<ExpectedTopicConfiguration> expectedConfig = configParser.parseTopicConfiguration(configFile);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        ComparisonResult result = new TopicComparer(props).compare(expectedConfig);
        return result;
    }

    private Collection<ExpectedTopicConfiguration> parseTopicConfiguration(String configFileLocation) {


        return configParser.parseTopicConfiguration(configFileLocation);
    }


    @Bean
    public ComparisonResultEvaluator clubTopicanaEvaluator(){
        return new ComparisonResultEvaluator(clubTopicanaComparisonResult());
    }


}
