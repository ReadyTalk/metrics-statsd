/**
 * Copyright (C) 2013 Michael Keesey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.bealetech.metrics.reporting;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;

public class StatsdSerializerTest {
    private static final String FAKE_METRIC_NAME = "foo.bar";
    private static final String SECOND_FAKE_METRIC_NAME = "dead.beef";
    private static final long INT_VAL = 3;
    private static final long SECOND_INT_VAL = 21;

    private enum TestPair {
        FIRST_PAIR(FAKE_METRIC_NAME, INT_VAL),
        SECOND_PAIR(SECOND_FAKE_METRIC_NAME, SECOND_INT_VAL);

        private final String name;
        private final long value;

        private TestPair(String name, long value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public String getValueAsString() {
            return Long.toString(value);
        }

        public long getValue() {
            return value;
        }
    }

    private StringWriter writer;
    private StatsdSerializer serializer;

    @Before
    public void init() {
        writer = new StringWriter();
    }

    private void createSerializerEmptyPrefix() {
        serializer = new StatsdSerializer("", writer);
    }

    private void verifyComponents(String output, StatType expectedMetricType, TestPair expected) {
        String[] components = output.split(":|\\|");
        Assert.assertEquals("Must start with metric name", expected.getName(), components[0]);
        Assert.assertEquals("Value must be correct", expected.getValueAsString(), components[1]);
        Assert.assertEquals("Type of metric must be correct", expectedMetricType.statsdType(), components[2]);
    }

    @Test
    public void testGauge() {
        createSerializerEmptyPrefix();
        TestPair pair = TestPair.FIRST_PAIR;
        serializer.writeGauge(pair.getName(), pair.getValue());
        verifyComponents(writer.toString(), StatType.GAUGE, pair);
    }

    @Test
    public void testMultipleGauges() {
        createSerializerEmptyPrefix();
        for (TestPair pair : TestPair.values()) {
            serializer.writeGauge(pair.getName(), pair.getValue());
        }

      String[] results = writer.toString().split("\n");

        for (TestPair verifyPair: TestPair.values()) {
            verifyComponents(results[verifyPair.ordinal()], StatType.GAUGE, verifyPair);
        }
    }

    @Test
    public void testTimer() {
        createSerializerEmptyPrefix();
        TestPair pair = TestPair.FIRST_PAIR;
        serializer.writeTimer(pair.getName(), pair.getValue());
        verifyComponents(writer.toString(), StatType.TIMER, pair);
    }

    @Test
    public void testMultipleTimers() {
        createSerializerEmptyPrefix();
        for (TestPair pair : TestPair.values()) {
            serializer.writeTimer(pair.getName(), pair.getValue());
        }

        String[] results = writer.toString().split("\n");
        System.out.println(writer.toString().getBytes().length);

        for (TestPair verifyPair: TestPair.values()) {
            verifyComponents(results[verifyPair.ordinal()], StatType.TIMER, verifyPair);
        }
    }

    //TODO test prefixes
}
