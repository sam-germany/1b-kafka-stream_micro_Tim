package com.course.kafka.util;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderPatternMessage;
import com.course.kafka.broker.message.OrderRewardMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Base64;

public class CommodityStreamUtil {

    public static OrderMessage maskCreditCard(OrderMessage original) {
        var converted = original.copy22();
        var maskedCreditCardNumber = original.getCreditCardNumber()
                                                    .replaceFirst("\\d{12}", StringUtils.repeat('*', 12));
        converted.setCreditCardNumber(maskedCreditCardNumber);
        return converted;
    }

    public static OrderPatternMessage mapToOrderPattern(OrderMessage original) {
        var result  = new OrderPatternMessage();

        result.setItemName(original.getItemName());
        result.setOrderDateTime(original.getOrderDateTime());
        result.setOrderLocation(original.getOrderLocation());
        result.setOrderNumber(original.getOrderNumber());

        var totalItemAmount = original.getPrice() * original.getQuantity();
        result.setTotalItemAmount(totalItemAmount);

        return result;
    }

    public static OrderRewardMessage mapToOrderReward(OrderMessage original) {
        var result = new OrderRewardMessage();

        result.setItemName(original.getItemName());
        result.setOrderDateTime(original.getOrderDateTime());
        result.setOrderLocation(original.getOrderLocation());
        result.setOrderNumber(original.getOrderNumber());
        result.setPrice(original.getPrice());
        result.setPrice(original.getPrice());
        result.setQuantity(original.getQuantity());

        return result;
    }

    public static Predicate<String, OrderMessage> isLargeQuantity()
    {
        return (key,value) -> value.getQuantity() > 200;
    }
//.startsWithIgnoreCase(source data, "checking word") <-- if the source data starts with Plastic then true else false
    public static Predicate<? super String, ? super OrderPatternMessage> isPlastic() {
        return (key, value) -> StringUtils.startsWithIgnoreCase(value.getItemName(), "Plastic");
    }

    public static Predicate<? super String, ? super OrderMessage> isCheap() {
        return (key, value) -> value.getPrice() < 100;
    }
// see video 86   8.30 time he defines this method
    public static KeyValueMapper<String, OrderMessage, String> generateStorageKey() {
        return (key, value) -> Base64.getEncoder().encodeToString(value.getOrderNumber().getBytes());
    }
//  <String, OrderMessage, KeyValue<String, OrderRewardMessage>>    means  <original key, original value, new key-value>
    public static KeyValueMapper<String, OrderMessage, KeyValue<String, OrderRewardMessage>> mapToOrderRewardChangeKey() {
        return (key, value) -> KeyValue.pair(value.getOrderLocation(), mapToOrderReward(value));
    }

}
