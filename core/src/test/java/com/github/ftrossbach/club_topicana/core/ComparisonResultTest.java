package com.github.ftrossbach.club_topicana.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created by ftr on 17.11.17.
 */
public class ComparisonResultTest {


    @Test
    @DisplayName("When there are no errors set, the result is ok")
    public void ok(){
        ComparisonResult comparisonResult = new ComparisonResult.ComparisonResultBuilder().build();
        assertTrue(comparisonResult.ok());
    }

    @Test
    @DisplayName("When there are is a missing topic, the result is not ok")
    public void missing_topic(){
        ComparisonResult comparisonResult = new ComparisonResult.ComparisonResultBuilder().addMissingTopic("hurz").build();
        assertFalse(comparisonResult.ok());
    }

    @Test
    @DisplayName("When there are is a wrong replication factor, the result is not ok")
    public void wrong_rf(){

        ComparisonResult comparisonResult = new ComparisonResult.ComparisonResultBuilder().addMismatchingReplicationFactor("hurz", 1, 2).build();
        assertFalse(comparisonResult.ok());
    }

    @Test
    @DisplayName("When there are is a wrong partition count, the result is not ok")
    public void wrong_broker_count(){

        ComparisonResult comparisonResult = new ComparisonResult.ComparisonResultBuilder().addMismatchingPartitionCount("hurz", 1, 2).build();
        assertFalse(comparisonResult.ok());
    }

    @Test
    @DisplayName("When there are is a wrong configuration, the result is not ok")
    public void wrong_config(){

        ComparisonResult comparisonResult = new ComparisonResult.ComparisonResultBuilder().addMismatchingConfiguration("hurz", "burz", "2", "3").build();
        assertFalse(comparisonResult.ok());
    }
}
