package com.course.kafka.broker.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

// see in the copy with diagram explained
public class CustomJsonSerializer<T> implements Serializer<T> {

    private ObjectMapper objectMapper22 = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {

        try {
            System.out.println(topic+" --Topic  "+data+" --Data  from serialize");
            return objectMapper22.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }
}
