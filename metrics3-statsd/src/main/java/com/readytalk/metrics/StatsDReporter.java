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

import com.codahale.metrics.*;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A reporter which publishes metric values to a StatsD server.
 *
 * @see <a href="https://github.com/etsy/statsd">StatsD</a>
 */
@NotThreadSafe
public class StatsDReporter extends ScheduledReporter {
  public static final String COUNT_POSTFIX_DEFAULT = "samples";

  private static final Logger LOG = LoggerFactory.getLogger(StatsDReporter.class);

  private final StatsD statsD;
  private final String prefix;
  private final MetricFilter filter;
  private final boolean diffCounters;
  private final ConcurrentHashMap<String, AtomicLong> counterCache = new ConcurrentHashMap<String, AtomicLong>();
  private final String countPostfix;
  private final boolean appendCountPostfixToCounters;

  public StatsDReporter(final MetricRegistry registry,
                         final StatsD statsD,
                         final String prefix,
                         final TimeUnit rateUnit,
                         final TimeUnit durationUnit,
                         final MetricFilter filter,
                         final boolean filterOnReporter,
                         final boolean diffCounters,
                         final String countPostfix,
                         final boolean appendCountPostfixToCounters) {
    super(registry, "statsd-reporter", filterOnReporter ? MetricFilter.ALL : filter, rateUnit, durationUnit);
    this.statsD = statsD;
    this.prefix = prefix;
    this.filter = filterOnReporter ? filter : MetricFilter.ALL;
    this.diffCounters = diffCounters;
    this.countPostfix = countPostfix;
    this.appendCountPostfixToCounters = appendCountPostfixToCounters;
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
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private boolean filterOnReporter;
    private boolean diffCounters;
    private String countPostfix;
    private boolean appendCountPostfixToCounters;

    private Builder(final MetricRegistry registry) {
      this.registry = registry;
      this.prefix = null;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.filterOnReporter = false;
      this.diffCounters = false;
      this.countPostfix = COUNT_POSTFIX_DEFAULT;
      this.appendCountPostfixToCounters = false;
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
     * By default filters are applied to the registry, this allows you to filter out entire metrics.  If however you
     * want to filter parts of a metric e.g. .p50 then you should use this option to apply the filter to the reporter
     * which will be called for all individual metric parts.
     * @return {@code this}
     */
    public Builder filterOnReporter() {
      this.filterOnReporter = true;
      return this;
    }

    /**
     * The default behavior for .count (applicable to {@link com.codahale.metrics.Timer},
     * {@link com.codahale.metrics.Meter}, {@link com.codahale.metrics.Histogram}, {@link com.codahale.metrics.Counter})
     * is that they continue to increase, this sets the reporter to report the difference vs the previously reported
     * value... useful for graphing results.
     *
     * @return {@code this}
     */
    public Builder diffCounters() {
      this.diffCounters = true;
      return this;
    }

    /**
     * Override the default post-fix {@value StatsDReporter#COUNT_POSTFIX_DEFAULT} for counting values.
     *
     * @return {@code this}
     */
    public Builder countPostfix(final String postFix) {
      this.countPostfix = postFix;
      return this;
    }

    /**
     * By default the count post-fix is not appended to {@link com.codahale.metrics.Counter}s,
     * use this option to set the reporter to append it.
     *
     * @return {@code this}
     */
    public Builder appendCountPostfixToCounters() {
      this.appendCountPostfixToCounters = true;
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
      return new StatsDReporter(
              registry,
              statsD,
              prefix,
              rateUnit,
              durationUnit,
              filter,
              filterOnReporter,
              diffCounters,
              countPostfix,
              appendCountPostfixToCounters);
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

  protected void reportTimer(final String name, final Timer timer) {
    final Snapshot snapshot = timer.getSnapshot();

    send(timer, prefix(name, "max"), formatNumber(convertDuration(snapshot.getMax())));
    send(timer, prefix(name, "mean"), formatNumber(convertDuration(snapshot.getMean())));
    send(timer, prefix(name, "min"), formatNumber(convertDuration(snapshot.getMin())));
    send(timer, prefix(name, "stddev"), formatNumber(convertDuration(snapshot.getStdDev())));
    send(timer, prefix(name, "p50"), formatNumber(convertDuration(snapshot.getMedian())));
    send(timer, prefix(name, "p75"), formatNumber(convertDuration(snapshot.get75thPercentile())));
    send(timer, prefix(name, "p95"), formatNumber(convertDuration(snapshot.get95thPercentile())));
    send(timer, prefix(name, "p98"), formatNumber(convertDuration(snapshot.get98thPercentile())));
    send(timer, prefix(name, "p99"), formatNumber(convertDuration(snapshot.get99thPercentile())));
    send(timer, prefix(name, "p999"), formatNumber(convertDuration(snapshot.get999thPercentile())));

    reportMetered(name, timer);
  }

  protected void reportMetered(final String name, final Metered meter) {
    send(meter, prefix(name, countPostfix), formatNumber(countingValue(name, meter)));
    send(meter, prefix(name, "m1_rate"), formatNumber(convertRate(meter.getOneMinuteRate())));
    send(meter, prefix(name, "m5_rate"), formatNumber(convertRate(meter.getFiveMinuteRate())));
    send(meter, prefix(name, "m15_rate"), formatNumber(convertRate(meter.getFifteenMinuteRate())));
    send(meter, prefix(name, "mean_rate"), formatNumber(convertRate(meter.getMeanRate())));
  }

  protected void reportHistogram(final String name, final Histogram histogram) {
    final Snapshot snapshot = histogram.getSnapshot();
    send(histogram, prefix(name, countPostfix), formatNumber(countingValue(name, histogram)));
    send(histogram, prefix(name, "max"), formatNumber(snapshot.getMax()));
    send(histogram, prefix(name, "mean"), formatNumber(snapshot.getMean()));
    send(histogram, prefix(name, "min"), formatNumber(snapshot.getMin()));
    send(histogram, prefix(name, "stddev"), formatNumber(snapshot.getStdDev()));
    send(histogram, prefix(name, "p50"), formatNumber(snapshot.getMedian()));
    send(histogram, prefix(name, "p75"), formatNumber(snapshot.get75thPercentile()));
    send(histogram, prefix(name, "p95"), formatNumber(snapshot.get95thPercentile()));
    send(histogram, prefix(name, "p98"), formatNumber(snapshot.get98thPercentile()));
    send(histogram, prefix(name, "p99"), formatNumber(snapshot.get99thPercentile()));
    send(histogram, prefix(name, "p999"), formatNumber(snapshot.get999thPercentile()));
  }

  protected void reportCounter(final String name, final Counter counter) {
    String sendName = appendCountPostfixToCounters ? prefix(name, countPostfix) : prefix(name);
    send(counter, sendName, formatNumber(countingValue(name, counter)));
  }

  @SuppressWarnings("rawtypes") //Metrics 3.0 passes us the raw Gauge type
  protected void reportGauge(final String name, final Gauge gauge) {
    final String value = format(gauge.getValue());
    if (value != null) {
      send(gauge, prefix(name), value);
    }
  }

  private long countingValue(final String name, final Counting counting) {

    if (!diffCounters) {
      return counting.getCount();
    }

    AtomicLong cachedCount = counterCache.get(name);

    if (cachedCount == null) {
      AtomicLong newCachedCount = new AtomicLong(0);
      cachedCount = counterCache.putIfAbsent(name, newCachedCount);
      if (cachedCount == null) {
        cachedCount = newCachedCount;
      }
    }

    long newCount = counting.getCount();
    return newCount - cachedCount.getAndSet(newCount);
  }

  protected void send(final Metric metric, final String name, final String value) {
    if (!filter.matches(name, metric)) {
      return;
    }
    statsD.send(name, value);
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
