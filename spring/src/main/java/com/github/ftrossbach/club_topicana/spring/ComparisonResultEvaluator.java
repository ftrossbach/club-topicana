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
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import javax.annotation.PostConstruct;


public class ComparisonResultEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(ComparisonResultEvaluator.class);

    @Value("${club-topicana.fail-on-mismatch:true}")
    private boolean failOnMismatch;

    private final ComparisonResult result;


    public ComparisonResultEvaluator(ComparisonResult result) {
        this.result = result;
    }


    @PostConstruct
    public void evaluate(){
        if(result.ok()){
            //nothing to do
        } else {

            if(failOnMismatch){
                throw new MismatchedTopicConfigException(result);
            } else {
                LOG.error("Kafka Topic configuration mismatched: " + result.toString());
            }

        }
    }
}
