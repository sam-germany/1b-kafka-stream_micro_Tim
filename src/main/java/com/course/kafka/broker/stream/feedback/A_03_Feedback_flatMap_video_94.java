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
public class A_03_Feedback_flatMap_video_94 {
    private static final Set<String> BAD_WORDS = Set.of("angry", "sad", "bad");
    private static final Set<String> GOOD_WORDS = Set.of("happy", "good", "helpful");

    @Bean
    public KStream<String, FeedbackMessage> kstreamFeedback(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

        var sourceStream = builder.stream("t.commodity.feedback", Consumed.with(stringSerde,feedbackSerde));

        var feedbackStreams = sourceStream.flatMap(splitWords22())
                                                              .branch(isGoodWord22(), isBadWord22());

        feedbackStreams[0].to("t.commodity.feedback-three-good");
        feedbackStreams[1].to("t.commodity.feedback-three-bad");

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

    private Predicate<? super String, ? super String> isBadWord22()
    {
        return (k,v) -> BAD_WORDS.contains(v);
    }


}


