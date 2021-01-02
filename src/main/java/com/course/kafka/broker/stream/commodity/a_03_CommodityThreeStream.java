package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderPatternMessage;
import com.course.kafka.broker.message.OrderRewardMessage;
import com.course.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class a_03_CommodityThreeStream {

    @Bean
    public KStream<String, OrderMessage> kstreamCommodityTreading(StreamsBuilder builder) {
         var stringSerde = Serdes.String();
         var orderSerde = new JsonSerde<>(OrderMessage.class);
         var orderPatternSerde = new JsonSerde<>(OrderPatternMessage.class);
         var orderRewardSerde = new JsonSerde<>(OrderRewardMessage.class);

         KStream<String,OrderMessage> maskedOrderStream
                 = builder.stream("t.commodity.order", Consumed.with(stringSerde,orderSerde))
                          .mapValues(CommodityStreamUtil::maskCreditCard);

//video 88  to understand this method just go to "CommodityTwoStream" class and see the same method in simple form written
         final var branchProducer = Produced.with(stringSerde, orderPatternSerde);
         new KafkaStreamBrancher<String, OrderPatternMessage>()
                .branch(CommodityStreamUtil.isPlastic(), x -> x.to("t.commodity.pattern-three.plastic", branchProducer))
                .defaultBranch(x -> x.to("t.commodity.pattern-three.notplastic", branchProducer))
                .onTopOf(maskedOrderStream.mapValues(CommodityStreamUtil::mapToOrderPattern));
/*Note: easy to understand, we are creating  a predefined class object "KafkaStreamBranch"  it is specially designed for
splitting Data, here first   .onTopOf() method will be executed first eventhough it is written at end, 3 steps are their
step 1)here first we are fetching the Stream data by  .onTopOc(maskedOrderStream....)  and in this method and convert
       the data into OrderPatternMessage by ::mapToOrderPattern  method
step 2).branch()  at second step this method will be executed. and take only the data as per Predicate return true
step 3) .defaultBranch()  as in the .branch() will take the data as per Predicate return true and the rest data
       will be returned back and we are retrieving the data by .defaultBranch() method and send further to topic
 */


         //using filter() and filterNot() together
        KStream<String, OrderRewardMessage> rewardStream = maskedOrderStream.filter(CommodityStreamUtil.isLargeQuantity())
                .filterNot(CommodityStreamUtil.isCheap()).mapValues(CommodityStreamUtil::mapToOrderReward);
        rewardStream.to("t.commodity.reward-three", Produced.with(stringSerde, orderRewardSerde));

        //sink stream to storage topic
        KStream<String, OrderMessage> storageStream = maskedOrderStream.selectKey(CommodityStreamUtil.generateStorageKey());
        storageStream.to("t.commodity.storage-three", Produced.with(stringSerde, orderSerde));


        return maskedOrderStream;
    }



}
