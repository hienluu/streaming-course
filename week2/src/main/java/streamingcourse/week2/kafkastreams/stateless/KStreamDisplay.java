package streamingcourse.week2.kafkastreams.stateless;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;
import static streamingcourse.common.KafkaCommonProperties.TWEETS_TOPIC_NAME;

/**
 * Simple example of using Kafka Streams to display the content of a topic.
 *
 * To reset the application offset so it will read from the beginning of the topic
 * kafka-streams-application-reset.sh --application-id KStreamDisplay-app --bootstrap-servers kafka1:29092,kafka2:29093,kafka3:29094 --input-topics streaming.week2.tweets
 */
public class KStreamDisplay {
    public static final String WORD_COUNT_INPUT_TOPIC_NAME = TWEETS_TOPIC_NAME;

    private static Logger log = LogManager.getLogger(KStreamDisplay.class.getName());
    public static void main(final String[] args) throws Exception {
        log.info("============== KStreamDisplay.main ============= ");
        log.info("reading lines from:  " + WORD_COUNT_INPUT_TOPIC_NAME);
        log.info("===================================================== ");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStreamDisplay-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "KStreamDisplay-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        StreamsBuilder builder = new StreamsBuilder();
        // create a stream from the WORD_COUNT_INPUT_TOPIC_NAME
        KStream<String, String> lineStream = builder.stream(WORD_COUNT_INPUT_TOPIC_NAME);

        lineStream.peek((key, value) -> log.info(String.format("key: %s, value: %s", key, value)));

        Topology topology = builder.build();
        log.info("topology: "  + topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, props);

        // reset the Kafka Streams app. state
        log.info("Performing clean up");
        streams.cleanUp();

        // Now run the processing topology via `start()` to begin processing its input data.
        log.info("Start running the topology");
        streams.start();

        // shutdown hook to properly and gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
