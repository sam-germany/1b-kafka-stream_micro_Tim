package com.course.kafka.broker.stream.orderpayment;

import com.course.kafka.broker.message.OnlineOrderMessage;
import com.course.kafka.broker.message.OnlineOrderPaymentMessage;
import com.course.kafka.broker.message.OnlinePaymentMessage;
import com.course.kafka.util.OnlineOrderTimestampExtractor22;
import com.course.kafka.util.OnlinePaymentTimestampExtractor22;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

// video 115
//@Configuration
public class A_02_OrderPaymentTwoStream_Left_Join {

    @Bean
    public KStream<String, OnlineOrderMessage> kstreamOrderPayment(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var orderSerde = new JsonSerde<>(OnlineOrderMessage.class);
        var paymentSerde = new JsonSerde<>(OnlinePaymentMessage.class);
        var orderPaymentSerde = new JsonSerde<>(OnlineOrderPaymentMessage.class);

 var orderStream11 = builder.stream("t.commodity.online-order",
                                    Consumed.with(stringSerde, orderSerde, new OnlineOrderTimestampExtractor22(), null));

 var paymentStream22 = builder.stream("t.commodity.online-payment",
                                    Consumed.with(stringSerde, paymentSerde, new OnlinePaymentTimestampExtractor22(),null ));

        // join
        orderStream11.leftJoin( paymentStream22,
                            this::jointOrderPayment22,
                            JoinWindows.of(Duration.ofHours(1)),
                            StreamJoined.with(stringSerde, orderSerde, paymentSerde)
                      ).to("t.commodity.join-order-payment-two", Produced.with(stringSerde, orderPaymentSerde));

        return orderStream11;
    }

    private OnlineOrderPaymentMessage jointOrderPayment22(OnlineOrderMessage order, OnlinePaymentMessage payment) {
         var result22 = new OnlineOrderPaymentMessage();

         result22.setOnlineOrderNumber(order.getOnlineOrderNumber());
         result22.setOrderDateTime(order.getOrderDateTime());
         result22.setTotalAmount(order.getTotalAmount());
         result22.setUsername(order.getUsername());

         if (payment != null) {
             result22.setPaymentDateTime(payment.getPaymentDateTime());
             result22.setPaymentMethod(payment.getPaymentMethod());
             result22.setPaymentNumber(payment.getPaymentNumber());
         }
         return result22;
    }
}
/*
(1)
builder.stream(... , ... , new OnlineOrderTimestampExtractor22(), null)

as we are putting here  "new OnlineOrderTimestampExtractor22()"   this will call the extract() method of the TimestampExtractor class
and put  LocalDateTime as per the logic in the message here.
(2)
rest is easy to understand that first we are fetching the data from 2 different topics and after that joining the data
and putting it in one class, this all done it by .join() method and after that we are sending the data to another topic


 */
