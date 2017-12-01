package com.github.ftrossbach.club_topicana.spring;

import com.github.ftrossbach.club_topicana.core.ComparisonResult;
import com.github.ftrossbach.club_topicana.core.MismatchedTopicConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import javax.annotation.PostConstruct;


public class ComparisonResultEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(ComparisonResultEvaluator.class);

    @Value("${club-topicana.fail-on-mismatch:false}")
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
