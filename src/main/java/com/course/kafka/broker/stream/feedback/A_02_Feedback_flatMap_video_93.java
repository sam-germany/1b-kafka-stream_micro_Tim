package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

//@Configuration
public class A_02_Feedback_flatMap_video_93 {
    private static final Set<String> GOOD_WORDS = Set.of("happy", "good", "helpful");

    @Bean
    public KStream<String, String> kstreamFeedback(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

var goodFeedbackStream
         =  builder.stream("t.commodity.feedback", Consumed.with(stringSerde,feedbackSerde))
                   .flatMap( (k,v) -> Arrays.asList(v.getFeedback().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+"))
                                            .stream()
                                            .filter(x -> GOOD_WORDS.contains(x))
                                            .distinct()
                                            .map(x -> KeyValue.pair(v.getLocation(),x))
                                            .collect(Collectors.toList()));

       goodFeedbackStream.to("t.commodity.feedback-two-good");
        return goodFeedbackStream;
    }
}
/* .flatMap(k,v)  as we want to change both  key-value so we are using .flatMap()

rest is easy to understand just go through from line 26 easy to understand this logic

 */


