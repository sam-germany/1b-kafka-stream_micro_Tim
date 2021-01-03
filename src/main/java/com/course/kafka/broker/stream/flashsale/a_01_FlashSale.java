package com.course.kafka.broker.stream.flashsale;

import com.course.kafka.broker.message.FlashSaleVoteMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class a_01_FlashSale {
    private static final Logger LOG = LoggerFactory.getLogger(a_01_FlashSale.class);

    @Bean
    public KStream<String, String> kstreamFlashSaleVote(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var flashSaleVoteSerde = new JsonSerde<>(FlashSaleVoteMessage.class);

        var flashSaleVoteStream
                = builder.stream("t.commodity.flashsale.vote", Consumed.with(stringSerde, flashSaleVoteSerde))
                        .map((key, value) -> KeyValue.pair(value.getCustomerId(), value.getItemName()));

        flashSaleVoteStream.print(Printed.<String, String>toSysOut().withLabel("Stream---------"));

        flashSaleVoteStream.to("t.commodity.flashsale.vote-user-item");

        // table
        builder.table("t.commodity.flashsale.vote-user-item", Consumed.with(stringSerde, stringSerde))
               .groupBy((user, votedItem) -> KeyValue.pair(votedItem, votedItem))
               .count()
                .toStream()
               .print(Printed.<String, Long>toSysOut().withLabel("Stream--+++++++++++-"));
          //   .to("t.commodity.flashsale.vote-one-result", Produced.with(stringSerde, Serdes.Long()));

        return flashSaleVoteStream;
    }
}
/*(1)
var flashSaleVoteStream = builder.stream("t.commodity.flashsale.vote", Consumed.with(stringSerde, flashSaleVoteSerde));

if we use this then  flashSaleVoteStream will get     KStream<String, FlashSaleVoteMessage>
-----------------------------------------------------------------------------------------
(2)
var flashSaleVoteStream = builder.stream("t.commodity.flashsale.vote", Consumed.with(stringSerde, flashSaleVoteSerde))
                        .map((key, value) -> KeyValue.pair(value.getCustomerId(), value.getItemName()));

her we are manipulating the return type with .map() and here the key-value  and after that wee are sending
the manipulated data into "t.commodity.flashsale.vote-user-item"
------------------------------------------------------------------------------------------
(3)
builder.table("t.commodity.flashsale.vote-user-item", Consumed.with(stringSerde, stringSerde))

here it is returning    (customerId , itemName)
-------------------------------------------------------------------------------------------
(4)
.groupBy((user, votedItem) -> KeyValue.pair(votedItem, votedItem))

it means    .groupBy((customerId, itemName) -> KeyValue.pair(itemName, itemName))

here we are grouping on the bases of  (itemName)
---------------------------------------------------------------------------------------------
(5)
 .count()        <--  it return the key-value as   (itemName, Long count)

 it means if we have 10 times same name then after 10 time the output will  (itemName, 10)
Note: this output will be only printed/returned when the incoming name will be changed
if the same name is coming then it will continue counting it will not return any value
only when the  itemName is changed then only it will return the value for old group

.count()  method return a Long value
----------------------------------------------------------------------------------------------
(6)
.toStream()

as we want to  send the data to the further Topic so we need to convert it to Stream

 */