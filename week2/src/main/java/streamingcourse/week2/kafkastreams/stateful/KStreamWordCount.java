package streamingcourse.week2.kafkastreams.stateful;


import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.logging.log4j.LogManager;


import java.util.Arrays;
import java.util.Properties;

import java.util.regex.Pattern;

import static streamingcourse.common.KafkaCommonProperties.*;

public class KStreamWordCount {
    private static org.apache.logging.log4j.Logger log = LogManager.getLogger(KStreamWordCount.class.getName());

    public static final String WORD_COUNT_INPUT_TOPIC_NAME = "week2-wordcount-input";
    public static final String WORD_COUNT_OUTPUT_TOPIC_NAME = "week2-wordcount-output";

    public static void main(final String[] args) throws Exception {
        log.info("============== KStreamWordCount.main ============= ");
        log.info("reading lines from:  " + WORD_COUNT_INPUT_TOPIC_NAME);
        log.info("writing word count results to:  " + WORD_COUNT_OUTPUT_TOPIC_NAME);

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

        log.info("APPLICATION_ID_CONFIG:  " + props.get(StreamsConfig.APPLICATION_ID_CONFIG));

        StreamsBuilder builder = new StreamsBuilder();
        // create a stream from the WORD_COUNT_INPUT_TOPIC_NAME
        KStream<String, String> lineStream = builder.stream(WORD_COUNT_INPUT_TOPIC_NAME);

        // regex pattern to split each line into words
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        // each record would contain k as null and v as the word
        KStream<String,String> wordStream = lineStream
                .flatMapValues(line -> Arrays.asList(pattern.split(line.toLowerCase())));

        KStream<String,String> noStopWordStream = wordStream.filter((key,value) -> !value.equals("the"));
        KGroupedStream<String, String> groupByGroupedStream = noStopWordStream.groupBy((key, word) -> word);
        // aggregation operators will always produce a KTable
        KTable<String,Long> wordCountTable = groupByGroupedStream.count();

        // write the result to output topic
        wordCountTable.toStream()
                .mapValues(value -> Long.toString(value))
                .to(WORD_COUNT_OUTPUT_TOPIC_NAME);

        log.info("Building topology");
        Topology topology = builder.build();
        log.info("topology: "  + topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, props);

        // Now run the processing topology via `start()` to begin processing its input data.
        log.info("Start running the topology");
        streams.cleanUp();
        streams.start();

        // shutdown hook to properly and gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
