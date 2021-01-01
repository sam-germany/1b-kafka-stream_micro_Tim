package com.course.kafka.broker.serde;

import com.course.kafka.broker.message.PromotionMessage;

// see in the copy with diagram explained
public class PromotionSerde extends CustomJsonSerde<PromotionMessage>{

    public PromotionSerde() {
        super(new CustomJsonSerializer<PromotionMessage>(),
                    new CustomJsonDeserializer<PromotionMessage>(PromotionMessage.class));




    }
}
