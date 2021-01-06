package com.course.kafka.util;

import com.course.kafka.broker.message.WebLayoutVoteMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class WebLayoutVoteTimestampExtractor22 implements TimestampExtractor {


    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var message = (WebLayoutVoteMessage) record.value();

        return message != null ?
                LocalDateTimeUtil22.toEpochTimestamp22(message.getVoteDateTime()) : record.timestamp();
    }
}
/*
message.getVoteDateTime()  <-- this is the Timestamp that we are passing as payload with the request

record.timestamp()                    <-- this is the Timestamp when this message is created on the Kafka.
                                    main point is this is added by default by the Kafka at the time of creating
                                    this message in the kafka cluster
 */