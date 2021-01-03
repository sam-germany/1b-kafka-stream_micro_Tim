package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

//@Configuration
public class A_06_Feedback_groupByKey_count_video_95 {
    private static final Set<String> BAD_WORDS = Set.of("angry", "sad", "bad");
    private static final Set<String> GOOD_WORDS = Set.of("happy", "good", "helpful");

    @Bean
    public KStream<String, FeedbackMessage> kstreamFeedback(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

        var sourceStream = builder.stream("t.commodity.feedback", Consumed.with(stringSerde,feedbackSerde));

        var feedbackStreams = sourceStream.flatMap(splitWords22())
                                                              .branch(isGoodWord22(), isBadWord22());

        feedbackStreams[0].to("t.commodity.feedback-five-good");
        feedbackStreams[0].groupByKey().count().toStream().to("t.commodity.feedback-five-good-count");

        feedbackStreams[1].to("t.commodity.feedback-five-bad");
        feedbackStreams[1].groupByKey().count().toStream().to("t.commodity.feedback-five-bad-count");


// getting total count of all good-words, bad-words, every time a word goes in at 0th index will be counted
        feedbackStreams[0].groupBy((k,v) -> v).count().toStream().to("t.commodity.feedback-six-good-count-word");
        feedbackStreams[1].groupBy((k,v) -> v).count().toStream().to("t.commodity.feedback-six-bad-count-word");

        return sourceStream;
    }

    private KeyValueMapper<String, FeedbackMessage, Iterable<KeyValue<String,String>>> splitWords22() {
        return (k, v) -> Arrays.asList(v.getFeedback().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+"))
                               .stream()
                               .distinct()
                               .map(x -> KeyValue.pair(v.getLocation(), x))
                               .collect(Collectors.toList());
    }

    private Predicate<? super String, ? super String> isGoodWord22() {

        return (k,v) -> GOOD_WORDS.contains(v);
    }

    private Predicate<? super String, ? super String> isBadWord22() {

        return (k,v) -> BAD_WORDS.contains(v);
    }
}
/*
feedbackStreams[0].groupByKey().count().toStream().to("t.commodity.feedback-four-good-count");

this part need to understand

.groupByKey()   <-- as the   key = v.getLocation()   so if the record have same key will be put in
                   one group and at the end the count will be displayed of records in every group
.count()        <-- it is a KTable specific method here the result is stored in KTable and after
                    that it count the values, null key or null value will be ignored and it return
                    Long data type
 */


