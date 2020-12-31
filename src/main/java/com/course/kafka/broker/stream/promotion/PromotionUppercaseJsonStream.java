package com.course.kafka.broker.stream.promotion;

import com.course.kafka.broker.message.PromotionMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;

//@Configuration
public class PromotionUppercaseJsonStream {
    private static final Logger LOG  = LoggerFactory.getLogger(PromotionUppercaseJsonStream.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public KStream<String, String> kstreamPromotionUppercase(StreamsBuilder builder) {
         var stringSerde = Serdes.String();
     KStream<String,String> sourceStream =
             builder.stream("t.commodity.promotion", Consumed.with(stringSerde,stringSerde));

     KStream<String,String> uppercaseStream22 = sourceStream.mapValues(this::uppercasePromotionCode22);
     uppercaseStream22.to("t.commodity.promotion-uppercase");

     sourceStream.print(Printed.<String,String>toSysOut().withLabel("JSON original Stream"));
     uppercaseStream22.print(Printed.<String,String>toSysOut().withLabel("JSON uppercase Stream"));

     return  sourceStream;
    }

    private String uppercasePromotionCode22(String message){
        try {
            var original = objectMapper.readValue(message, PromotionMessage.class);
            var converted22 = new PromotionMessage(original.getPromotionCode().toUpperCase());
            return objectMapper.writeValueAsString(converted22);

        } catch (JsonProcessingException e) {
            LOG.warn("cannot process message {} ", message);
        }

        return StringUtils.EMPTY;
    }




}
