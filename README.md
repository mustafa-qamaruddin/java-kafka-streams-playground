# 🚀 Kafka Stream Superstar! 🌟

Introducing my passion project: the **Kafka Stream Consumer App** 🎉

## What's This All About?

Ever wondered how to handle streaming data like a rockstar? This Java application, powered by the amazing Apache Kafka Streams library, does just that! It's all about crunching real-time data, calculating average values, and streaming the results to another Kafka topic. 🎸

## Prerequisites

To ride the wave with this app, you'll need:

1. Java Development Kit (JDK) 8 or higher.
2. An Apache Kafka cluster revved up and ready to roll. Don't worry; we've got `localhost:29092` to get you started. 🚗💨

## Ready, Set, Jam!

Follow these steps to unleash the power of the **Kafka Stream Consumer**:

1. Git-groove into action by cloning this repo:

```bash
git clone https://github.com/your-username/kafka-stream-consumer.git
```

2. Get into the rhythm with your favorite Java IDE.

## How to Be a Stream Rockstar

Run the `main` method in the `org.qubits.consumer.Consumer` class to kickstart this party! 🎵

Our app will jam with the data flowing in from the Kafka topic named `movies`. It'll crank up the volume by calculating the average value per key (where the key is an `Integer`, and the value is a `Double`). Then, we'll send the groovy results to the `movies-enriched` Kafka topic.

Once the beats are on, there's no stopping until you hit the pause button! 💃

## Mix It Up!

You're the DJ of your own experience! Tweak the knobs and turn up the fun by adjusting these properties in the `Consumer` class:

- `StreamsConfig.APPLICATION_ID_CONFIG`: Name your own show!
- `StreamsConfig.BOOTSTRAP_SERVERS_CONFIG`: The party address of Kafka brokers.
- `StreamsConfig.NUM_STREAM_THREADS_CONFIG`: Number of crew members in this show!

Customize to impress and make it uniquely yours! 🎶

## License to Thrill

Feeling the freedom to rock? Play it, share it, and go wild with it! 🤘

So, are you ready to tap your foot to the rhythm of Kafka Streams? 🎧 Let's make some noise and get the spotlight on your Kafka Stream superstar skills! 🌟