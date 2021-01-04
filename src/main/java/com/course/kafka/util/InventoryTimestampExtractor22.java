package com.course.kafka.util;

import com.course.kafka.broker.message.InventoryMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class InventoryTimestampExtractor22 implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var inventoryMessage = (InventoryMessage) record.value();

 //       System.out.println(inventoryMessage.getTransactionTime()+"--#########");
//        System.out.println(record.timestamp()+"+++++++++++");

        return inventoryMessage != null ? LocalDateTimeUtil22.toEpochTimestamp22(inventoryMessage.getTransactionTime())
                : record.timestamp();
    }
}
/*

inventoryMessage.getTransactionTime()  <-- this is the Timestamp that we are passing as payload with the request

record.timestamp()                    <-- this is the Timestamp when this message is created on the Kafka.
                                    main point is this is added bydefault by the Kafka at the time of creating
                                    this message in the kafka cluster



 */
