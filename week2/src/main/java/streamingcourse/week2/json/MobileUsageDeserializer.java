package streamingcourse.week2.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import streamingcourse.week2.json.MobileUsage;

import java.util.Map;

public class MobileUsageDeserializer <T> implements Deserializer<T> {
    private transient ObjectMapper objectMapper;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
        objectMapper = JsonMapper.builder().build(); //.registerModule();
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            return (T)objectMapper.readValue(bytes, MobileUsage.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Encountered error while deserializing bytes into "
                    + MobileUsage.class.getName(), e);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
