package com.course.kafka.broker.stream.web;

import com.course.kafka.broker.message.WebColorVoteMessage;
import com.course.kafka.broker.message.WebDesignVoteMessage;
import com.course.kafka.broker.message.WebLayoutVoteMessage;
import com.course.kafka.util.WebColorVoteTimestampExtractor22;
import com.course.kafka.util.WebLayoutVoteTimestampExtractor22;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;


// video  118
//@Configuration
public class A_02_WebDesignVoteTwoStream_Left_Join {

    @Bean
    public KStream<String, WebDesignVoteMessage> kstreamWebDesignVote(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var colorSerde = new JsonSerde<>(WebColorVoteMessage.class);
        var layoutSerde = new JsonSerde<>(WebLayoutVoteMessage.class);
        var designSerde = new JsonSerde<>(WebDesignVoteMessage.class);

//color
builder.stream("t.commodity.web.vote-color", Consumed.with(stringSerde, colorSerde, new WebColorVoteTimestampExtractor22(), null))
       .mapValues( v -> v.getColor()).to("t.commodity.web.vote-two-username-color");
//color Table
var colorTable = builder.table("t.commodity.web.vote-two-username-color",Consumed.with(stringSerde, stringSerde));

//layout
builder.stream("t.commodity.web.vote-layout", Consumed.with(stringSerde, layoutSerde, new WebLayoutVoteTimestampExtractor22(), null))
                .mapValues( v -> v.getLayout()).to("t.commodity.web.vote-two-username-layout");
//layout Table
var layoutTable = builder.table("t.commodity.web.vote-two-username-layout",Consumed.with(stringSerde, stringSerde));

// join color-Table  + layout-Table
var joinTable22 = colorTable.leftJoin(layoutTable, this::voteJoiner22, Materialized.with(stringSerde, designSerde));
joinTable22.toStream().to("t.commodity.web.vote-two-result");

       joinTable22.groupBy((username, votedDesign) -> KeyValue.pair(votedDesign.getColor(), votedDesign.getColor()))
                  .count().toStream().print(Printed.<String, Long>toSysOut().withLabel("vote two - color"));

       joinTable22.groupBy((username, votedDesign) -> KeyValue.pair(votedDesign.getLayout(), votedDesign.getLayout()))
                 .count().toStream().print(Printed.<String, Long>toSysOut().withLabel("vote two - Layout"));

        return joinTable22.toStream();
    }

    private WebDesignVoteMessage voteJoiner22(String color, String layout) {
          var result = new WebDesignVoteMessage();

          result.setColor(color);
          result.setLayout(layout);

          return  result;
    }
}
/*
(1)
Note: form the kafka-order producer when we are sending data to kafka we are sending extra argument as "key = username"
as key is unique in both the topics   Color and Layout so from this unique key we are joining the table and
showing result according the unique key
(2)
"builder.stream("t.commodity.web.vote-color",  ..........  .mapValues( v -> v.getColor())"

here we are creating a Stream and the key is already sended as "username" and here we are setting the value to the key
Key=username  value = v.getColor()  <-- this all done by .mapValues() method and the important this while using the
.mapValue() method is that we do not need to pass key here, the key i already defined and internally pass this value
to the key
(3)
var joinTable22 =  = colorTable.leftJoin(layoutTable, this::voteJoiner22)       <--  little bit trickey, as when we are
retrieving data from 2 topics and in the memory we are creating a KTable as we are creating KStream
so in that table when the data come with same key=username  from the 2 Topics will be stored in the Ktable
and we are also storing the  color-layout in the KTable
---------------------------------------------------------------------------------------------------------------------
(4)Rule:
KTable will create a record when their must be a value choosed for Left side and Right side can choose a value
or it can also be null, in both cases for LeftJoin KTable will create a record

if in the begining Left side have a value and Right side is null then a recored is created by KTable,
but if at later point if Right side choose a value then again a new record is created with values for both sides

Note: if we try to create again same record with same value it will not create any new record

see the video 118  time 2:09  their he show chart, clearly show this point


---------------------------------------------------------------------------------------------------------------------

 */
