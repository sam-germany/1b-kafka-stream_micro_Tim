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
// video 105
//@Configuration
public class A_01_InventoryOneStream_aggregate {

    @Bean
    public KStream<String, InventoryMessage> kstreamInventory(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var inventorySerde = new JsonSerde<>(InventoryMessage.class);
        var longSerde = Serdes.Long();

        var inventoryStream = builder.stream("t.commodity.inventory", Consumed.with(stringSerde, inventorySerde));

        inventoryStream.mapValues((k,v) -> v.getQuantity())
                       .groupByKey()
//                                ()-> 0l,("abc" ,    4   ,   0l     )->  0      +    4    ,                                           )
                       .aggregate(()-> 0l,(aggkey, newValue, aggValue)-> aggValue+ newValue, Materialized.with(stringSerde, longSerde))
                       .toStream()
                       .to("t.commodity.inventory-total-one", Produced.with(stringSerde,longSerde));

        return inventoryStream;
    }
}
/*
(1)
it is easy to understand  form kafka-order application we are getting  key=item and value=InventoryMessage.class
(2)
inventoryStream.mapValues((k,v) -> v.getQuantity())       <-- here key=item  but value=quantity, here we are changing the value type
(3)
.aggregate()   <-- above explained the .aggregate() functionality, it returns   KTable
(4)
.toStream()    <-- as we are getting KTable so we need to convert from KTable to KStream then only we can send through network to
                  another Topic
 */