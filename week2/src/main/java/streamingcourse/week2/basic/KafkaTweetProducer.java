package streamingcourse.week2.basic;


import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import static streamingcourse.week2.KafkaCommonProperties.*;

/**
 * Simple Kafka producer send messages in string format (key,value)
 *
 * It will send NUM_MSGS_TO_SEND to Kafka topic KAFKA_TOPIC_TO_SEND_TO.
 * It will sleep for SLEEP_TIME_IN_MS between each message
 */
public class KafkaTweetProducer {
    private static final String KAFKA_STRING_SERIALIZER = StringSerializer.class.getName();
    private static final long SLEEP_TIME_IN_MS = TimeUnit.SECONDS.toMillis(1);
    private static final int NUM_MSGS_TO_SEND = 0;
    private static final String KAFKA_TOPIC_TO_SEND_TO = TWEETS_TOPIC_NAME;

    private static final Logger LOGGER = LogManager.getLogger(KafkaTweetProducer.class.getName());
    public static void main(String[] args) {
        Faker faker = new Faker();

        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaTweetProducer");

        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);

        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_STRING_SERIALIZER);

        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_STRING_SERIALIZER);



        //Create a Kafka producer from configuration
        KafkaProducer<String, String> simpleProducer = new KafkaProducer<String, String>(kafkaProps);

        //Publish NUM_MSGS_TO_SEND messages at SLEEP_TIME_IN_MS second intervals, with a random key
        try{
            int startKey = (new Random()).nextInt(100) ;
            LOGGER.info("startKey: " + startKey);
            LOGGER.info("NUM_MSGS_TO_SEND: " + NUM_MSGS_TO_SEND);
            LOGGER.info("... sleeping time in MS: " + SLEEP_TIME_IN_MS);

            for( int i=startKey; i <= startKey + NUM_MSGS_TO_SEND; i++) {
                String quote = faker.buffy().quotes();
                //Create a producer Record
                ProducerRecord<String, String> kafkaRecord =

                        new ProducerRecord<String, String>(
                                KAFKA_TOPIC_TO_SEND_TO,    //Topic name
                                String.valueOf(i),         //Key for the message
                                quote                      //Value: message Content
                        );

                LOGGER.info("Sending Message : "+ kafkaRecord.toString());
                //Publish to Kafka
                simpleProducer.send(kafkaRecord);
                Thread.sleep(SLEEP_TIME_IN_MS);
            }
        }
        catch(Exception e) {
            LOGGER.error("Got exception: " + e.getMessage(), e);
        }
        finally {
            simpleProducer.close();
        }
        LOGGER.info("Finish sending " + NUM_MSGS_TO_SEND + " messages");
    }
}
