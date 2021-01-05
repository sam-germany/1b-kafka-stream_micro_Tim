package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import com.course.kafka.util.InventoryTimestampExtractor22;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

// video 110        <-- somehow not understand this complete video lesson 110
//@Configuration
public class A_06_InventorySixStream_TimestampExtractor {

    @Bean
    public KStream<String, InventoryMessage> kstreamInventory(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var inventorySerde = new JsonSerde<>(InventoryMessage.class);
        var longSerde = Serdes.Long();

        var windowLength22 = Duration.ofHours(1);
        var hopLength22 = Duration.ofMinutes(20);
        var windowSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLength22.toMillis());

        var inventoryTimestampExtractor = new InventoryTimestampExtractor22();

   KStream<String, InventoryMessage> inventoryStream = builder.stream("t.commodity.inventory",
                                    Consumed.with(stringSerde, inventorySerde, inventoryTimestampExtractor, null));

        inventoryStream
                .mapValues((k, v) -> v.getType().equalsIgnoreCase("ADD") ? v.getQuantity() : -1 * v.getQuantity())
                .groupByKey()
                .windowedBy(TimeWindows.of(windowLength22).advanceBy(hopLength22))
                .reduce(Long::sum, Materialized.with(stringSerde, longSerde))
                .toStream()
                .through("t.commodity.inventory-total-five", Produced.with(windowSerde, longSerde))
                .print(Printed.toSysOut());

        return inventoryStream;
    }
}
/*
(1)
builder.stream(... , ... , new InventoryTimestampExtractor22(), null)

as we are putting here  "new InventoryTimestampExtractor22()"   this will call the extract() method of the TimestampExtractor class
and put  LocalDateTime as per the logic in the message here.
(2)
var windowSerde     <-- we need window serde  as a key, means just draw in mind as we want a result in this example
of Duration.ofHours(1) means record of 1 hour, so we need to define this WindowSerde as a key that specify the
time range. means this key of 1 hour and value is the result of 1 hour
(3)
.windowedBy(TimeWindows.of(windowLength))      <-- here we are defining the Time-period that  we need to group the
                                        result as per this time-period,means for 1 hour one result made, after that
                                        next hour again a new result is created so make a groups of 1 hour results
(4)
.through("t.commodity.inventory-total-five", Produced.with(windowSerde, longSerde))   <-- at the time of sending this
                                        result to another Topic we need to take the key as  "windowSerde"
 */


