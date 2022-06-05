package streamingcourse.week2.mobileusage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @deprecated see @code{KJsonDeserializer}
 * @param <T>
 */
public class MobileUsageSerializer <T> implements Serializer<T> {

    private ObjectMapper objectMapper;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
        objectMapper =
                JsonMapper.builder()
                        .build().findAndRegisterModules();
    }

    @Override
    public byte[] serialize(String topic, T t) {
        try {
            return objectMapper.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Encountered error while serializing: " + t.toString(), e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
