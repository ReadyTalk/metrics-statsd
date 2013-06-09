/**
 * Copyright (C) 2012-2013 Sean Laurent
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;

public class StatsdSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(StatsdSerializer.class);

    private final String prefix;
    private final Writer writer;

    private boolean prependNewline;

    public StatsdSerializer(String metricPrefix, Writer writer) {
        this.prefix = metricPrefix;
        this.writer = writer;
        this.prependNewline = false;
    }

    public void writeGauge(String metricName, long value) {
        writeGauge(metricName, Long.toString(value, 10));
    }

    public void writeGauge(String metricName, double value) {
        writeGauge(metricName, String.format("%2.2f", value));
    }

    private void writeGauge(String metricName, String value) {
        writeData(metricName, sanitizeString(value), StatType.GAUGE);
    }

    //TODO counters

    public void writeTimer(String metricName, long timeInMS) {
        //TODO bounds checking?
        writeData(metricName, Long.toString(timeInMS, 10), StatType.TIMER);
    }

    public void writeTimer(String metricName, double value) {
        //TODO bounds checking?
        writeData(metricName, String.format("%2.2f", value), StatType.TIMER);
    }

    private String sanitizeString(String s) {
        return s.replace(' ', '-');
    }

    private void writeData(String name, String value, StatType statType) {
        try {
            if (prependNewline) {
                writer.write("\n");
            }
            if (!prefix.isEmpty()) {
                writer.write(prefix);
            }
            writer.write(sanitizeString(name));
            writer.write(":");
            writer.write(value);
            writer.write("|");
            writer.write(statType.statsdType());
            prependNewline = true;
            writer.flush();
        } catch (IOException e) {
            LOG.error("Error sending to StatsD:", e);
        }
    }
}
