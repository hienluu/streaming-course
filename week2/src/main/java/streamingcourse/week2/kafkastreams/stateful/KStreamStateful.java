package streamingcourse.week2.kafkastreams.stateful;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import static streamingcourse.common.KafkaCommonProperties.BOOTSTRAP_SERVER_LIST;

/**
 * Examples of stateful operators
 */
public class KStreamStateful {
    private static final String WORD_COUNT_INPUT_TOPIC_NAME = "week2-wordcount-input";

    public static void main(final String[] args) throws Exception {
        System.out.println("============== KStreamStateful.main ============= ");
        System.out.println("reading lines from:  " + WORD_COUNT_INPUT_TOPIC_NAME);
        System.out.println("============== KStreamStateful.main ============= ");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-stateful-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "kstreams-stateful-app-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        StreamsBuilder builder = new StreamsBuilder();
        // create a stream from the WORD_COUNT_INPUT_TOPIC_NAME
        KStream<String, String> lineStream = builder.stream(WORD_COUNT_INPUT_TOPIC_NAME);

        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        KGroupedStream<String,String> groupedStream = lineStream
                .flatMapValues(line -> Arrays.asList(pattern.split(line.toLowerCase())))
                .groupBy((key, word) -> word);


        KStream<String, Long> countStream = groupedStream.count().toStream();

        KGroupedStream<String,Integer> wordCountPairStream = lineStream
                .flatMapValues(line -> Arrays.asList(pattern.split(line.toLowerCase())))
                .map((key, word) -> KeyValue.pair(word, 1))
                .groupByKey();

        KStream<String, Integer> reducedStream = wordCountPairStream.reduce((aggValue, newValue)  -> aggValue + newValue)
                                                                    .toStream();

        KStream<String, Long> aggregatedStream = wordCountPairStream.aggregate(() ->
                                                     0L /*initializer */,
                                                    (aggKey, newValue, aggValue) -> aggValue + newValue) /* adder */
                                                .toStream();

        System.out.println("Building topology");
        Topology topology = builder.build();
        System.out.println("topology: "  + topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, props);


        System.out.println("Start running the topology");
        streams.start();

        // shutdown hook to properly and gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
