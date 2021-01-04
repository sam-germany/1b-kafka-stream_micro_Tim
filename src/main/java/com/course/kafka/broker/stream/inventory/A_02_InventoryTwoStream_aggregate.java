package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

// video 106
//@Configuration
public class A_02_InventoryTwoStream_aggregate {

    @Bean
    public KStream<String, InventoryMessage> kstreamInventory(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var inventorySerde = new JsonSerde<>(InventoryMessage.class);
        var longSerde = Serdes.Long();

        var inventoryStream = builder.stream("t.commodity.inventory", Consumed.with(stringSerde, inventorySerde));

        inventoryStream.mapValues((k,v) -> v.getType().equalsIgnoreCase("ADD") ? v.getQuantity() : -1 * v.getQuantity())
                       .groupByKey()
//                                ()-> 0l,("abc" ,    4   ,   0l     )->  0      +    4    ,                                           )
                       .aggregate(()-> 0l,(aggkey, newValue, aggValue)-> aggValue+ newValue, Materialized.with(stringSerde, longSerde))
                       .toStream()
                       .to("t.commodity.inventory-total-two", Produced.with(stringSerde,longSerde));

        return inventoryStream;
    }
}
/*
  inventoryStream.mapValues((k,v) -> v.getType().equalsIgnoreCase("ADD") ? v.getQuantity() : -1 * v.getQuantity())

here from the  kafka-order  application InventoryService class  we are sending type=ADD or type=REMOVE
if type=ADD then  v.getQuantity() and we are late adding in the  .aggregate() function

if type=REMOVE  then  -1 * getQuantity()     means e.g    -1 * 2  = -2      so late when the call goes to
.aggregate() method then  it will minus the total value as   plus  minus  =  minus so we are subtracting the value
  */