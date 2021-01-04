package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingTwoMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

// video 104
//@Configuration
public class A_06_FeedbackRatingTwoStream {

    @Bean
    public KStream<String, FeedbackMessage> kstreamFeedbackRating(StreamsBuilder builder) {
        var  stringSerde = Serdes.String();     // key are   "location", "feedback", rating", "feedbackDataTime"
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);
        var feedbackRatingTwoSerde = new JsonSerde<>(FeedbackRatingTwoMessage.class);
        var feedbackRatingTwoStoreValueSerde = new JsonSerde<>(A_04_FeedbackRatingTwoStoreValue.class);

        var feedbackStream
                = builder.stream("t.commodity.feedback", Consumed.with(stringSerde,feedbackSerde));

        var feedbackRatingStateStoreName = "feedbackRatingTwoStateStore";
        var storeSupplier = Stores.inMemoryKeyValueStore(feedbackRatingStateStoreName);
        var storeBuilder
                 = Stores.keyValueStoreBuilder(storeSupplier, stringSerde,feedbackRatingTwoStoreValueSerde);

        builder.addStateStore(storeBuilder);

        feedbackStream.transformValues(() -> new A_05_FeedbackRatingTwoTransformer(feedbackRatingStateStoreName), feedbackRatingStateStoreName)
                      .to("t.commodity.feedback.rating-two", Produced.with(stringSerde,feedbackRatingTwoSerde));

        return  feedbackStream;
    }
}
/*the main point is at line 25  var feedbackStream  we are receiving the data and later we are adding old and new data
and producing the new Average and sending to a new topic
(1)
var feedbackRatingStateStoreName = "feedbackRatingOneStateStore";     <- here we are defining a name in the Store, with this
              name our data object is saved in the Store, when we want to call it then we have to use this name again
(2)
var storeSupplier = Stores.inMemoryKeyValueStore(feedbackRatingStateStoreName);  <-- here we are creating a Store with the
                                                        given name in the Store memory
(3)   var storeBuilder = .........  <-- here we are building the store with 3 parameters
  Stores.keyValueStoreBuilder(storeSupplier     , stringSerde   ,  feedbackRatingOneStoreValueSerde);
                             (already given name, Key="location", as value the storage reference name)
(4)
builder.addStateStore(storeBuilder);        <-- here we are adding the State of the Store to the StreamBuilder so that
 at line 25 we will get the whole references to this variable   "var feedbackStream"   just draw in mind that  from line
 28 till 33  will be defined for the line 25
(5)
feedbackStream.transformValues(() -> new A_02_FeedbackRatingOneTransformer(feedbackRatingStateStoreName), feedbackRatingStateStoreName)

this is simple to understand that we are calling this "new A_02_FeedbackRatingOneTransformer()" class instance and after that
what the result we are getting after it will be returned as  "feedbackRatingStateStoreName"

and at end we are sending the result to the  topic     "t.commodity.feedback.rating-one"
 */

