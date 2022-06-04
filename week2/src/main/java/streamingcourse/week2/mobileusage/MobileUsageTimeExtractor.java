package streamingcourse.week2.mobileusage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;


public class MobileUsageTimeExtractor implements TimestampExtractor {
    private static Logger log = LogManager.getLogger(MobileUsageTimeExtractor.class.getName());

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {

        MobileUsage mobileUsage = (MobileUsage) consumerRecord.value();
        /*String logMsg = String.format("user: %s event time: %s  prev time: %s",
                mobileUsage.getUserName(), mobileUsage.timeStamp, Instant.ofEpochSecond(l));
        log.info(logMsg);*/
        return mobileUsage.timeStamp.toEpochMilli();

    }
}
