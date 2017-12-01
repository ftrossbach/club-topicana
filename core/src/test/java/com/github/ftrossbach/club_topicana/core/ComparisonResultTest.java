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
