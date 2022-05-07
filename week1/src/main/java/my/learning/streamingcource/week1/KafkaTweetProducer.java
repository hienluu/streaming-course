package my.learning.streamingcource.week1;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Simple Kafka producer send messages in string format (key,value)
 *
 * It will send NUM_MSGS_TO_SEND to Kafka topic KAFKA_TOPIC_TO_SEND_TO.
 * It will sleep for SLEEP_TIME_IN_MS between each message
 */
public class KafkaTweetProducer {
    private static final String BOOTSTRAP_SERVER_LIST = "localhost:9092,localhost:9093,localhost:9094";
    private static final String KAFKA_STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final long SLEEP_TIME_IN_MS = TimeUnit.SECONDS.toMillis(2);
    private static final int NUM_MSGS_TO_SEND = 20;
    private static final String KAFKA_TOPIC_TO_SEND_TO = "streaming.week1.tweets";

    public static void main(String[] args) {

        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();

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
            System.out.println("startKey: " + startKey);
            System.out.println("NUM_MSGS_TO_SEND: " + NUM_MSGS_TO_SEND);
            System.out.println("... sleeping time in MS: " + SLEEP_TIME_IN_MS);

            for( int i=startKey; i <= startKey + NUM_MSGS_TO_SEND; i++) {

                //Create a producer Record
                ProducerRecord<String, String> kafkaRecord =
                        new ProducerRecord<String, String>(
                                KAFKA_TOPIC_TO_SEND_TO,    //Topic name
                                String.valueOf(i),          //Key for the message
                                "This is tweet message " + i         //Message Content
                        );

                System.out.println("Sending Message : "+ kafkaRecord.toString());
                //Publish to Kafka
                simpleProducer.send(kafkaRecord);
                Thread.sleep(SLEEP_TIME_IN_MS);
            }
        }
        catch(Exception e) {
            System.out.println("Got exception: " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            simpleProducer.close();
        }
    }
}
