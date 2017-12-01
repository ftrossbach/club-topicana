[![Build Status](https://travis-ci.org/ftrossbach/kiqr.svg?branch=master)](https://travis-ci.org/ftrossbach/club-topicana)
[![Coverage Status](https://coveralls.io/repos/github/ftrossbach/kiqr/badge.svg)](https://coveralls.io/github/ftrossbach/club-topicana)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Club Topicana
... check are free.


## What does it do?
This library allows you to check the configuration of the Kafka topics you depend
on before you produce or consume from them. This project was motivated by a situation in a project where Kafka
was managed by a central team that did not want to disable automatic topic creation. So at some point, we ended
up with a topic with just partition count and replication factor set to 1. Increasing a partition count is fairly easy, 
so folks did it right away. Replication factor? Not so much fun – you basically need to do a manual partition reassignment. 
Not fun for 50 partitions. 

Club Topicana allows you to specify your expected topic configuration in advance (programmatically or in a YAML file) and 
then allows you to execute a check if it matches the real topic configuration when you create a Kafka Producer, Consumer,
Stream or a Spring application

## Configuration

A YAML config looks like this
```
- name: test_topic
  replication-factor: 1
  partition-count: 1
  config:
    - cleanup.policy: delete
    - delete.retention.ms: 86400000

- name: test_topic2
  replication-factor: 1
  config:
    - compression.type: producer
    - file.delete.delay.ms: 60000

```

This can be parsed in the following way:
```
Collection<ExpectedTopicConfiguration> expectedConfig = new ConfigParser().parseTopicConfiguration("classpath-location-of-file");
```


The programmatic equivalent looks like this:

```
ExpectedTopicConfiguration testTopic = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic")
                                      .withReplicationFactor(1)
                                      .withPartitionCount(1)
                                      .withConfig("delete.retention.ms", "86400000")
                                      .withConfig("cleanup.policy", "delete")
                                      .build();
                                      
ExpectedTopicConfiguration testTopic2 = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic2")
                                      .withReplicationFactor(1)
                                      .withConfig("compression.type", "producer")
                                      .withConfig("file.delete.delay.ms", "60000")
                                      .build();

```

Every parameter is optional – if no partition count is specified, Club Topicana assumes you don't care. Config properties
are also only checked if they're specifically included


## Kafka Clients and Streams

Club Topicana contains factories that extend the default constructors of `KafkaProducer` and `KafkaConsumer` with another
parameter expecting a collection of `ExpectedTopicConfiguration`. If the config doesn't fit, those factory methods
will throw a MismatchedTopicConfigException` ìf something goes wrong.

Examples:


Producer:
```
ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic")
                                      .withReplicationFactor(2).build();
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
Producer<String,String> producer = KafkaProducerFactory.producer(props, Collections.singleton(expected));

```

Consumer:

```
ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
Consumer<String, String> consumer = KafkaConsumerFactory.consumer(props, new StringDeserializer(), new StringDeserializer(), Collections.singleton(expected));
```

Streams:

```
ExpectedTopicConfiguration expected = new ExpectedTopicConfiguration.ExpectedTopicConfigurationBuilder("test_topic").build();

Properties props = new Properties();
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
[..]
StreamsBuilder builder = new StreamsBuilder();
builder.<String, String>table("test_topic").groupBy((key, value) -> new KeyValue<>(key, value)).count("store");

KafkaStreams streams = KafkaStreamsFactory.streams(builder.build(), new StreamsConfig(props), Collections.singleton(expected));
```

For Kafka producers and consumers, you may depend on

```
<dependency>
    <groupId>com.github.ftrossbach</groupId>
    <artifactId>club-topicana-kafka-clients</artifactId>
    <version>0.1.0</version>
</dependency>
```

For Kafka Streams, you may use

```
<dependency>
    <groupId>com.github.ftrossbach</groupId>
    <artifactId>club-topicana-kafka-streams</artifactId>
    <version>0.1.0</version>
</dependency>
```


## Spring

For Spring applications, all you need to do is to use the "EnableClubTopicana" annotation:

```
@SpringBootApplication
@EnableClubTopicana
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }
}
```

These are the configuration options:

|Property                                     |Optional  |Default                         |
|---------------------------------------------|----------|--------------------------------|
|club-topicana.bootstrap-servers              |No        |None (Example: "localhost:9092" |
|club-topicana.config-file:club-topicana.yml  |Yes       |club-topicana.yml               |
|club-topicana.fail-on-mismatch:false         |Yes       |true                            |


You can include it in your project by adding

```
<dependency>
    <groupId>com.github.ftrossbach</groupId>
    <artifactId>club-topicana-spring</artifactId>
    <version>0.1.0</version>
</dependency>
```
