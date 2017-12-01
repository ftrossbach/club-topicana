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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigParser {
    public ConfigParser() {
    }

    /**
     * Parses a yaml file containing Club Topicana configuration
     * @param configFileLocation The classpath-relative location of the config file
     * @return
     */
    public Collection<ExpectedTopicConfiguration> parseTopicConfiguration(String configFileLocation) {
        List<Map<String, Object>> map = (List<Map<String, Object>>) new org.yaml.snakeyaml.Yaml().load(this.getClass().getClassLoader().getResourceAsStream(configFileLocation));


        return map.stream().map(entry -> {
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
                    Map<String, String> stringifiedConfig = configMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));
                    build.withConfig(stringifiedConfig);
                });

            }


            return build.build();

        }).collect(Collectors.toList());
    }
}