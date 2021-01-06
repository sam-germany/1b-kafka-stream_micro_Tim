package com.course.kafka.broker.stream.subscription;

import com.course.kafka.broker.message.SubscriptionOfferMessage;
import com.course.kafka.broker.message.SubscriptionPurchaseMessage;
import com.course.kafka.broker.message.SubscriptionUserMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

// video   124
//@Configuration
public class A_02_SubscriptionOfferTwoStream2 {

    @Bean
    public KStream<String, SubscriptionOfferMessage> kstreamSubscription(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var purchaseSerde = new JsonSerde<>(SubscriptionPurchaseMessage.class);
        var userSerde = new JsonSerde<>(SubscriptionUserMessage.class);
        var offerSerde = new JsonSerde<>(SubscriptionOfferMessage.class);

        var purchaseStream = builder.stream("t.commodity.subscription-purchase", Consumed.with(stringSerde, purchaseSerde));

        var userTable = builder.globalTable("t.commodity.subscription-user", Consumed.with(stringSerde, userSerde));

        // join Stream + Table                                                                                       ( key   , purchaseMessage, userMessage)
        var offerStream = purchaseStream.join(userTable, (key,value)-> key,this::joiner22);

      offerStream.to("t.commodity.subscription-offer-two", Produced.with(stringSerde, offerSerde));

      return offerStream;
    }

    private SubscriptionOfferMessage joiner22(SubscriptionPurchaseMessage purchase, SubscriptionUserMessage user) {
        var result = new SubscriptionOfferMessage();

        result.setUsername(purchase.getUsername());
        result.setSubscriptionNumber(purchase.getSubscriptionNumber());
        result.setDuration(user.getDuration());
        return result;
    }



}
