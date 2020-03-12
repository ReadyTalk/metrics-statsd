/**
 * Copyright (C) 2013 metrics-statsd contributors
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
package com.readytalk.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a StatsD server.
 *
 * @see <a href="https://github.com/etsy/statsd">StatsD</a>
 */
@NotThreadSafe
public class StatsDReporter extends ScheduledReporter {
  private static final Logger LOG = LoggerFactory.getLogger(StatsDReporter.class);

  private final StatsD statsD;
  private final String prefix;
  private final String suffix;

  private StatsDReporter(final MetricRegistry registry,
                         final StatsD statsD,
                         final String prefix,
                         final String suffix,
                         final TimeUnit rateUnit,
                         final TimeUnit durationUnit,
                         final MetricFilter filter) {
    super(registry, "statsd-reporter", filter, rateUnit, durationUnit);
    this.statsD = statsD;
    this.prefix = prefix;
    this.suffix = suffix;
  }

  /**
   * Returns a new {@link Builder} for {@link StatsDReporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link StatsDReporter}
   */
  public static Builder forRegistry(final MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder for {@link StatsDReporter} instances. Defaults to not using a prefix,
   * converting rates to events/second, converting durations to milliseconds, and not
   * filtering metrics.
   */
  @NotThreadSafe
  public static final class Builder {
    private final MetricRegistry registry;
    private String prefix;
    private String suffix;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;

    private Builder(final MetricRegistry registry) {
      this.registry = registry;
      this.prefix = null;
      this.suffix = null;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }

    /**
     * Prefix all metric names with the given string.
     *
     * @param _prefix the prefix for all metric names
     * @return {@code this}
     */
    public Builder prefixedWith(@Nullable final String _prefix) {
      this.prefix = _prefix;
      return this;
    }

    /**
     * Suffix all metric names with the given string.
     *
     * @param _suffix the suffix for all metric names
     * @return {@code this}
     */
    public Builder suffixedWith(@Nullable final String _suffix) {
      this.suffix = _suffix;
      return this;
    }
    /**
     * Convert rates to the given time unit.
     *
     * @param _rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(final TimeUnit _rateUnit) {
      this.rateUnit = _rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param _durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(final TimeUnit _durationUnit) {
      this.durationUnit = _durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param _filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(final MetricFilter _filter) {
      this.filter = _filter;
      return this;
    }

    /**
     * Builds a {@link StatsDReporter} with the given properties, sending metrics to StatsD at the given host and port.
     *
     * @param host the hostname of the StatsD server.
     * @param port the port of the StatsD server. This is typically 8125.
     * @return a {@link StatsDReporter}
     */
    public StatsDReporter build(final String host, final int port) {
      return build(new StatsD(host, port));
    }

    /**
     * Builds a {@link StatsDReporter} with the given properties, sending metrics using the
     * given {@link StatsD} client.
     *
     * @param statsD a {@link StatsD} client
     * @return a {@link StatsDReporter}
     */
    public StatsDReporter build(final StatsD statsD) {
      return new StatsDReporter(registry, statsD, prefix, suffix, rateUnit, durationUnit, filter);
    }
  }


  @Override
  @SuppressWarnings("rawtypes") //Metrics 3.0 interface specifies the raw Gauge type
  public void report(final SortedMap<String, Gauge> gauges,
                     final SortedMap<String, Counter> counters,
                     final SortedMap<String, Histogram> histograms,
                     final SortedMap<String, Meter> meters,
                     final SortedMap<String, Timer> timers) {

    try {
      statsD.connect();

      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        reportGauge(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        reportCounter(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
        reportHistogram(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<String, Meter> entry : meters.entrySet()) {
        reportMetered(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<String, Timer> entry : timers.entrySet()) {
        reportTimer(entry.getKey(), entry.getValue());
      }
    } catch (IOException e) {
      LOG.warn("Unable to report to StatsD", statsD, e);
    } finally {
      try {
        statsD.close();
      } catch (IOException e) {
        LOG.debug("Error disconnecting from StatsD", statsD, e);
      }
    }
  }

  private void reportTimer(final String name, final Timer timer) {
    final Snapshot snapshot = timer.getSnapshot();

    statsD.send(suffix(prefix(name, "max")), formatNumber(convertDuration(snapshot.getMax())));
    statsD.send(suffix(prefix(name, "mean")), formatNumber(convertDuration(snapshot.getMean())));
    statsD.send(suffix(prefix(name, "min")), formatNumber(convertDuration(snapshot.getMin())));
    statsD.send(suffix(prefix(name, "stddev")), formatNumber(convertDuration(snapshot.getStdDev())));
    statsD.send(suffix(prefix(name, "p50")), formatNumber(convertDuration(snapshot.getMedian())));
    statsD.send(suffix(prefix(name, "p75")), formatNumber(convertDuration(snapshot.get75thPercentile())));
    statsD.send(suffix(prefix(name, "p95")), formatNumber(convertDuration(snapshot.get95thPercentile())));
    statsD.send(suffix(prefix(name, "p98")), formatNumber(convertDuration(snapshot.get98thPercentile())));
    statsD.send(suffix(prefix(name, "p99")), formatNumber(convertDuration(snapshot.get99thPercentile())));
    statsD.send(suffix(prefix(name, "p999")), formatNumber(convertDuration(snapshot.get999thPercentile())));

    reportMetered(name, timer);
  }

  private void reportMetered(final String name, final Metered meter) {
    statsD.send(suffix(prefix(name, "samples")), formatNumber(meter.getCount()));
    statsD.send(suffix(prefix(name, "m1_rate")), formatNumber(convertRate(meter.getOneMinuteRate())));
    statsD.send(suffix(prefix(name, "m5_rate")), formatNumber(convertRate(meter.getFiveMinuteRate())));
    statsD.send(suffix(prefix(name, "m15_rate")), formatNumber(convertRate(meter.getFifteenMinuteRate())));
    statsD.send(suffix(prefix(name, "mean_rate")), formatNumber(convertRate(meter.getMeanRate())));
  }

  private void reportHistogram(final String name, final Histogram histogram) {
    final Snapshot snapshot = histogram.getSnapshot();
    statsD.send(suffix(prefix(name, "samples")), formatNumber(histogram.getCount()));
    statsD.send(suffix(prefix(name, "max")), formatNumber(snapshot.getMax()));
    statsD.send(suffix(prefix(name, "mean")), formatNumber(snapshot.getMean()));
    statsD.send(suffix(prefix(name, "min")), formatNumber(snapshot.getMin()));
    statsD.send(suffix(prefix(name, "stddev")), formatNumber(snapshot.getStdDev()));
    statsD.send(suffix(prefix(name, "p50")), formatNumber(snapshot.getMedian()));
    statsD.send(suffix(prefix(name, "p75")), formatNumber(snapshot.get75thPercentile()));
    statsD.send(suffix(prefix(name, "p95")), formatNumber(snapshot.get95thPercentile()));
    statsD.send(suffix(prefix(name, "p98")), formatNumber(snapshot.get98thPercentile()));
    statsD.send(suffix(prefix(name, "p99")), formatNumber(snapshot.get99thPercentile()));
    statsD.send(suffix(prefix(name, "p999")), formatNumber(snapshot.get999thPercentile()));
  }

  private void reportCounter(final String name, final Counter counter) {
    statsD.send(suffix(prefix(name)), formatNumber(counter.getCount()));
  }

  @SuppressWarnings("rawtypes") //Metrics 3.0 passes us the raw Gauge type
  private void reportGauge(final String name, final Gauge gauge) {
    final String value = format(gauge.getValue());
    if (value != null) {
      statsD.send(suffix(prefix(name)), value);
    }
  }

  @Nullable
  private String format(final Object o) {
    if (o instanceof Float) {
      return formatNumber(((Float) o).doubleValue());
    } else if (o instanceof Double) {
      return formatNumber((Double) o);
    } else if (o instanceof Byte) {
      return formatNumber(((Byte) o).longValue());
    } else if (o instanceof Short) {
      return formatNumber(((Short) o).longValue());
    } else if (o instanceof Integer) {
      return formatNumber(((Integer) o).longValue());
    } else if (o instanceof Long) {
      return formatNumber((Long) o);
    } else if (o instanceof BigInteger) {
      return formatNumber((BigInteger) o);
    } else if (o instanceof BigDecimal) {
      return formatNumber(((BigDecimal) o).doubleValue());
    }
    return null;
  }

  private String prefix(final String... components) {
    return MetricRegistry.name(prefix, components);
  }

  private String suffix(final String string) {
    if (suffix != null && !suffix.isEmpty()) {
      return string + suffix;
    }
    return string;
  }

  private String formatNumber(final BigInteger n) {
    return String.valueOf(n);
  }

  private String formatNumber(final long n) {
    return Long.toString(n);
  }

  private String formatNumber(final double v) {
    return String.format(Locale.US, "%2.2f", v);
  }
}
