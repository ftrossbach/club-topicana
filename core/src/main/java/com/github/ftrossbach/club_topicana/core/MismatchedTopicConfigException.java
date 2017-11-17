package com.github.ftrossbach.club_topicana.core;

public class MismatchedTopicConfigException extends RuntimeException {


    public MismatchedTopicConfigException(ComparisonResult result){
        super("Topic configuration does not match specification: " +result.toString());
    }
}
