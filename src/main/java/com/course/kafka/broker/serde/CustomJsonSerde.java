package com.course.kafka.broker.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

// see in the copy with diagram explained
public class CustomJsonSerde<T> implements Serde<T> {

    private final CustomJsonSerializer<T> serializer22;
    private final CustomJsonDeserializer<T> deserializer22;

    public CustomJsonSerde(CustomJsonSerializer<T> serializer, CustomJsonDeserializer<T> deserializer) {
        System.out.println(serializer +" from serializer Serde+++++ "+ deserializer+" from deserializer Serde+++++");
        this.serializer22 = serializer;
        this.deserializer22 = deserializer;
    }

    @Override
    public Serializer serializer() {
        return serializer22;
    }

    @Override
    public Deserializer deserializer() {
        return deserializer22;
    }
}
