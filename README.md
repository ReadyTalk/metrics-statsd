# metrics-statsd

StatsD reporter for codahale/metrics. Supports versions 2 and 3.

## Quick Start

```java
MetricRegistry registry = new MetricRegistry();
StatsDReporter.forRegistry(registry)
    .build("statsd.example.com", 8125)
    .start(10, TimeUnit.SECONDS);
```

## Gradle

```groovy
repositories {
  mavenRepo(url: 'http://dl.bintray.com/readytalk/maven')
}

compile('com.readytalk:metrics3-statsd:3.X.X')
```

## Maven

Instructions for including metrics-statsd into a maven project can be found on the [bintray repository](https://bintray.com/readytalk/maven/metrics-statsd).

## Credits

This is based off of Sean Laurent's [metrics-statsd](https://github.com/organicveggie/metrics-statsd) and the graphite module of [Coda Hale's Metrics](https://github.com/codahale/metrics)
