package com.course.kafka.broker.stream.flashsale;

import com.course.kafka.broker.message.FlashSaleVoteMessage;
import com.course.kafka.util.LocalDateTimeUtil22;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.LocalDateTime;

public class A_02_ValueTransformer implements ValueTransformer<FlashSaleVoteMessage, FlashSaleVoteMessage> {

    private final long voteStartTime;
    private final long voteEndTime;
    private ProcessorContext processorContext;

    public A_02_ValueTransformer(LocalDateTime voteStart, LocalDateTime voteEnd) {
        this.voteStartTime = LocalDateTimeUtil22.toEpochTimestamp22(voteStart);
        this.voteEndTime = LocalDateTimeUtil22.toEpochTimestamp22(voteEnd);
    }

    @Override
    public void init(ProcessorContext context) {

       this.processorContext = context;
    }

    @Override
    public FlashSaleVoteMessage transform(FlashSaleVoteMessage value) {
        var recordTime  = processorContext.timestamp();
        return (recordTime >= voteStartTime && recordTime <= voteEndTime) ? value: null;
    }

    @Override
    public void close() {
    }
}
/*
see video 102   <-- here we are getting the ProcessorContext from the ProcessorApi after that we
are converting the LocalDateTime into Epoch millisecond by using LocalDateTimeUtil22 and putting
that converted time into the ProcessorContext of the ProcessorApi
 */
