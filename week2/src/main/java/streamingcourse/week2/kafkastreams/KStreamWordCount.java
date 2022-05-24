package streamingcourse.week2.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import static streamingcourse.week2.KafkaCommonProperties.*;

public class KStreamWordCount {
    public static final String WORD_COUNT_OUTPUT_TOPIC_NAME = "streaming-week2-wordcount-output";

    public static void main(final String[] args) throws Exception {
        System.out.println("============== KStreamWordCount.main ============= ");
        System.out.println("reading tweets from:  " + TWEETS_TOPIC_NAME);
        System.out.println("writing word count results to:  " + WORD_COUNT_OUTPUT_TOPIC_NAME);
        System.out.println("============== KStreamWordCount.main ============= ");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-wordcount-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // create a stream from the TWEETS_TOPIC_NAME
        KStream<String, String> tweets = builder.stream(TWEETS_TOPIC_NAME);

        // regex pattern to split each tweet into words
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        KTable<String, Long> wordCounts = tweets
                .flatMapValues(tweet -> Arrays.asList(pattern.split(tweet.toLowerCase())))
                .groupBy((key, word) -> word)
                .count();

        wordCounts.toStream().to(WORD_COUNT_OUTPUT_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Now run the processing topology via `start()` to begin processing its input data.
        streams.start();

        // shutdown hook to properly and gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
