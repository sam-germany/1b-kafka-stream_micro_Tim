package com.course.kafka.broker.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Objects;


// see in the copy with diagram explained 
public class CustomJsonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper objectMapper22 = new ObjectMapper();
    private final Class<T> deserializedClass;

    public  CustomJsonDeserializer(Class<T> deserializedClass) {
        Objects.requireNonNull(deserializedClass,"Deserialized class must not null");
        System.out.println(deserializedClass +"  --Deserialized class");
        this.deserializedClass = deserializedClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {

        try {
            System.out.println(topic+" --Topic  "+data+" --Data from deserialize");
            return objectMapper22.readValue(data, deserializedClass);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
