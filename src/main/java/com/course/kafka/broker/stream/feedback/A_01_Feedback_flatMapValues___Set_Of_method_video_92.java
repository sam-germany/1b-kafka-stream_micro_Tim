package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

//@Configuration
public class A_01_Feedback_flatMapValues___Set_Of_method_video_92 {
    private static final Set<String> GOOD_WORDS = Set.of("happy", "good", "helpful");

    @Bean
    public KStream<String, String> kstreamFeedback(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

        var goodFeedbackStream
               =  builder.stream("t.commodity.feedback", Consumed.with(stringSerde,feedbackSerde))
                         .flatMapValues(mapperGoodWords22()); // main point to understand is the values which
                                                       // we are changing in the mapperGoodWords22() will only
                                           //be changed  rest all the data   remain same as it is coming, only
                                                          //what we are changing will be changed in the Stream
       goodFeedbackStream.to("t.commodity.feedback-one-good");
        return goodFeedbackStream;
    }

    private ValueMapper<FeedbackMessage, Iterable<String>> mapperGoodWords22() {

  //      return   x1 -> Arrays.asList(x1.getFeedback().replaceAll("[^a-zA-Z]]", "")
        return   x1 -> Arrays.asList(x1.getFeedback().replaceAll("[^a-zA-Z]]", "")
                                                   .toLowerCase()
                                                   .split("\\s+"))
                                                   .stream()
                                                   .peek(x -> System.out.println(x))
                                                   .filter(x -> GOOD_WORDS.contains(x))
                                                   .distinct()

                                                   .collect(Collectors.toList());
    }
}
/*
builder.stream()   <-- it will give us a Array of key-values of the given deserializer object
Arrays.asList()   <-- we are converting Array values into a List, as we can manipulate the list with
                      many methods
x1.getFeedback().replaceAll("[^a-zA-Z]]", ""    <-- here we are getting all the value of "feedback" in
                    as one line  and here we do .replaceAll() means only allow alphabets a-z or A-Z
                    rest all character or numbers should be removed
.split("\\s+"))   <- here every empty space in between characters will push to a new line
.stream()    <-- as we can create a Stream from List of values
.filter(x -> GOOD_WORD.contains(x))   <-- here in the filter we are allowing those words that
                   are equal to words defined  GOOD_WORDS
.distinct()    <-- means no duplicate allowess
.collect(Collectors.toList());   <-- as we have Stream of values and now we are converting that
                Stream into a  Iterable List. as our return type is also Iterable<String>

Set.of() method
---------------
The Set.of is a static factory method that creates immutable Set introduced in Java 9. The instance
   created by Set.of has following characteristics.
1. The Set obtained from Set.of is unmodifiable. We cannot add, delete elements to it. We can also not update the
   reference of object at any index in the unmodifiable Set.
2. An unmodifiable Set is immutable only when it contains immutable objects. Immutable Set is automatically
   thread-safe. It also consumes much less memory than the mutable one.
3. If we try to change unmodifiable Set, it will throw UnsupportedOperationException.
4. The immutable Set can be used to improve performance and save memory.
5. The unmodifiable Set does not allow duplicate values and null values at creation time.
6. The iteration order of Set elements are unspecified and may change.
7. The Set.of has following method signature.
 */
