package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import com.course.kafka.util.InventoryTimestampExtractor22;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
// video 108
//@Configuration
public class A_04_InventoryFourStream_TimestampExtractor {

    @Bean
    public KStream<String, InventoryMessage> kstreamInventory(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var inventorySerde = new JsonSerde<>(InventoryMessage.class);
        var inventoryTimestampExtractor22 = new InventoryTimestampExtractor22();

        var inventoryStream = builder.stream("t.commodity.inventory",
                           Consumed.with(stringSerde, inventorySerde, inventoryTimestampExtractor22, null));

        inventoryStream.to("t.commodity.inventory-four", Produced.with(stringSerde, inventorySerde));

        return inventoryStream;
    }
}

/*

see the video from 108  and also see the InventoryTimestampExtractor22 class  their i explain the
logic difference

 */


