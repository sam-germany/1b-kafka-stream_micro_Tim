package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingTwoMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;
import java.util.Optional;

public class A_05_FeedbackRatingTwoTransformer implements ValueTransformer<FeedbackMessage, FeedbackRatingTwoMessage> {
   private ProcessorContext processorContext;
   private final String stateStoreName;
   private KeyValueStore<String, A_04_FeedbackRatingTwoStoreValue> ratingStateStore;

    public A_05_FeedbackRatingTwoTransformer(String stateStoreName ) {
        if (StringUtils.isEmpty(stateStoreName)) { throw new IllegalArgumentException("State store name must not empty"); }

     this.stateStoreName  = stateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.processorContext = context;
this.ratingStateStore
        = (KeyValueStore<String, A_04_FeedbackRatingTwoStoreValue>) this.processorContext.getStateStore(this.stateStoreName);
    }               // here we are retrieving from the Stores the FeedbackRatingOneStoreVale  properties  at the time of init()
                        // method call first time then it return null but see down in transform method if it is null then their
                        //we are creating new Object as    "new A_01_FeedbackRatingOneStoreValue()" if this object is already
                          // their with some data then we are fetching the data here at this init() method call

    @Override
    public FeedbackRatingTwoMessage transform(FeedbackMessage feedbackMessage) { // in the FeedbackRatingOne class
                                          //we are having feedbackStream, in this Stream we are consuming topic with datatype
                                                // FeedbackMessage so form their we are getting this as argument

        // as calculation is based on  "location" so we are checking that perviously that location already exists or not
        // if location exists then fetch the old data and add to new if not exists then just create a dummy object for it
var storeValue = Optional.ofNullable(ratingStateStore.get(feedbackMessage.getLocation()))
                                                            .orElse(new A_04_FeedbackRatingTwoStoreValue());

        var ratingMap = storeValue.getRatingMap(); // if "location" already exists then here we are
                                                 // trying to fetch the old data, if not exists then here we will get null

        //it is little trickey to understand as we are getting output at the end is  how many times which rating comes
        //e.g  rating "5"=2  means 2 user gives 2 times 5-5 rating so here we are using
        //  ratingMap.get(feedbackMessage.getRating())  <-- means   Map.get(key) means we are searching for this
        //rating if it already exist then we will get the old counts if not then we will start it from zero
        // to better understand just run this application and see the result and then easy to understand
        var currentRatingCount = Optional.ofNullable(ratingMap.get(feedbackMessage.getRating()))
                                               .orElse(0l);
        var newRatingCount = currentRatingCount + 1;
        ratingMap.put(feedbackMessage.getRating(), newRatingCount);// here we are simply using
          // Map.put() method, it will override the old value with the newRatingCount for the
          // same key, here key is the rating and value is the how many time user choose it

        ratingStateStore.put(feedbackMessage.getLocation(), storeValue);

        // send this message to sink topic
        var branchRating = new FeedbackRatingTwoMessage();
        branchRating.setLocation(feedbackMessage.getLocation());
        branchRating.setAverageRating(calculateAverage22(ratingMap));
        branchRating.setRatingMap(ratingMap);

        return branchRating;
    }

    private double calculateAverage22(Map<Integer,Long> ratingMap) {
        var sumRating = 01;
        var countRating = 0;

        for( var entry : ratingMap.entrySet() )  {
            sumRating += entry.getKey() * entry.getValue();
            countRating += entry.getKey();
        }

        return  Math.round((double) sumRating / countRating * 10d) / 10d;
    }

    @Override
    public void close() {

    }
}
