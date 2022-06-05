package streamingcourse.week2.mobileusage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import streamingcourse.week2.mobileusage.model.MobileUsage;

import java.util.Map;

/**
 * @deprecated
 * Serdes for MobileUsage custom record type
 * @param <T>
 *
 */
public class MobileUsageSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private transient ObjectMapper objectMapper;

    public MobileUsageSerde() {
        objectMapper = JsonMapper.builder().build(); //.registerModule();
        objectMapper.findAndRegisterModules();
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
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
        objectMapper = JsonMapper.builder().build(); //.registerModule();
        objectMapper.findAndRegisterModules();
    }

    @Override
    public byte[] serialize(String s, T t) {
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
