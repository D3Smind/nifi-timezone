/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.desmind.processors.timezonelookup;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MyProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(TimeZoneLookup.class);
    }

    @Test
    public void testProcessor() {
        testRunner.setProperty("latitude attribute", "lat");
        testRunner.setProperty("longitude attribute", "lon");
        testRunner.setProperty("timestamps to convert", "ts");
        Map<String, String> properties = new HashMap<>();
        properties.put("lat", "53.45");
        properties.put("lon", "9.98");
        properties.put("ts", "2023-09-13T08:01:32Z");
        testRunner.enqueue("", properties);
        testRunner.run(1);

        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(TimeZoneLookup.REL_SUCCESS);
        List<MockFlowFile> results_fail = testRunner.getFlowFilesForRelationship(TimeZoneLookup.REL_FAILURE);
        if (!results_fail.isEmpty()){
            MockFlowFile resultingFF = results_fail.get(0);
            System.out.println(resultingFF.getAttribute("Fail"));
        }
        assertEquals(1, results.size());
        MockFlowFile resultingFF = results.get(0);
        assertEquals("Europe/Berlin", resultingFF.getAttribute("possible_zone"));
    }

}
