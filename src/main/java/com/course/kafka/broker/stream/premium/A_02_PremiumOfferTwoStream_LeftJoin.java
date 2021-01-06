package com.course.kafka.broker.stream.premium;


import com.course.kafka.broker.message.PremiumOfferMessage;
import com.course.kafka.broker.message.PremiumPurchaseMessage;
import com.course.kafka.broker.message.PremiumUserMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;

// video 122
//@Configuration
public class A_02_PremiumOfferTwoStream_LeftJoin {

    @Bean
    public KStream<String, PremiumOfferMessage> kstreamPremiumOffer(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var purchaseSerde = new JsonSerde<>(PremiumPurchaseMessage.class);
        var userSerde = new JsonSerde<>(PremiumUserMessage.class);
        var offerSerde = new JsonSerde<>(PremiumOfferMessage.class);

        var purchaseStream = builder.stream("t.commodity.premium-purchase", Consumed.with(stringSerde, purchaseSerde))
                 .selectKey((k,v) -> v.getUsername());

        var filterLevel = List.of("gold", "diamond");
        var userTable = builder.table("t.commodity.premium-user", Consumed.with(stringSerde, userSerde))
                .filter((k,v) -> filterLevel.contains(v.getLevel().toLowerCase()));

        // joining Stream + Table  will create a new Stream
        var offerStream = purchaseStream.leftJoin(userTable, this::joiner22, Joined.with(stringSerde, purchaseSerde, userSerde));

        offerStream.to("t.commodity.premium-offer-one", Produced.with(stringSerde, offerSerde));

        return offerStream;
    }

    private PremiumOfferMessage joiner22(PremiumPurchaseMessage purchase, PremiumUserMessage user) {
        var result = new PremiumOfferMessage();

        result.setUsername(purchase.getUsername());
        result.setPurchaseNumber(purchase.getPurchaseNumber());

        if(user != null) {
            result.setLevel(user.getLevel());
        }
        return  result;
    }
}
/*(1) at line 29   .selectKey((k,v) -> v.getUsername());

as before this method the key=purchaseNumber , this key is coming from the kakfa-order application, but here we are changing the key=userName
so after this method key=username only, the value which was before it is same

Note: joining  from  KStream to KTable  in outerJoin is not alloweed
 */

