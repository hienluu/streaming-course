package streamingcourse.week2.kafkastreams;


import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;


import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static streamingcourse.week2.KafkaCommonProperties.*;

public class KStreamWordCount {
    public static final String WORD_COUNT_INPUT_TOPIC_NAME = "week2-wordcount-input";
    public static final String WORD_COUNT_OUTPUT_TOPIC_NAME = "week2-wordcount-output";

    private static Logger log = Logger.getLogger(KStreamWordCount.class.getName());
    public static void main(final String[] args) throws Exception {
        System.out.println("============== KStreamWordCount.main ============= ");
        System.out.println("reading lines from:  " + WORD_COUNT_INPUT_TOPIC_NAME);
        System.out.println("writing word count results to:  " + WORD_COUNT_OUTPUT_TOPIC_NAME);
        System.out.println("============== KStreamWordCount.main ============= ");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-wordcount-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "kstreams-wordcount-app-client");
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

        // regex pattern to split each line into words
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        KStream<String,String> wordCounts = lineStream
                .flatMapValues(line -> Arrays.asList(pattern.split(line.toLowerCase())))
                .filter((key,value) -> !value.equals("the"))
                .groupBy((key, word) -> word)
                .count()
                        .mapValues(value -> Long.toString(value))
                                .toStream();

        wordCounts.to(WORD_COUNT_OUTPUT_TOPIC_NAME);

        System.out.println("Building topology");
        Topology topology = builder.build();
        System.out.println("topology: "  + topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, props);

        // Now run the processing topology via `start()` to begin processing its input data.
        System.out.println("Start running the topology");
        streams.cleanUp();

        streams.start();

        // shutdown hook to properly and gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
