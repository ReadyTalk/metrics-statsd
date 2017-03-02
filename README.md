# metrics-statsd [![Build Status](https://travis-ci.org/ReadyTalk/metrics-statsd.png?branch=master)](https://travis-ci.org/ReadyTalk/metrics-statsd)

StatsD reporter for dropwizard/metrics. Supports versions 2 and 3.

Requires Java 7+

## Quick Start

```java
MetricRegistry registry = new MetricRegistry();

// Using metrics2-statsd
StatsDReporter reporter = new StatsDReporter(registry,
    "statsd.example.com",
    8125,
    "metric.prefix");
reporter.start(10, TimeUnit.SECONDS);

// Using metrics3-statsd
StatsDReporter.forRegistry(registry)
    .build("statsd.example.com", 8125)
    .start(10, TimeUnit.SECONDS);
```

## Gradle

```groovy
repositories {
    jcenter()
}

// for Metrics 2.x
compile('com.readytalk:metrics2-statsd:4.1.0')
// for Metrics 3.x
compile('com.readytalk:metrics3-statsd:4.1.0')
```

## Maven

Instructions for including metrics-statsd into a maven project can be found on the [bintray repository](https://bintray.com/readytalk/maven/metrics-statsd).

## Building

```shell
./gradlew install #installs metrics2 and metrics3 libraries to your local maven repository
```

## Credits

This is based off of Sean Laurent's [metrics-statsd](https://github.com/organicveggie/metrics-statsd) and the graphite module of [Coda Hale's Metrics](https://github.com/codahale/metrics)
