package streamingcourse.week2.mobileusage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MobileUsageTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        MobileUsage mobileUsage = (MobileUsage) consumerRecord.value();
        return mobileUsage.timeStamp.toEpochMilli();

    }
}
