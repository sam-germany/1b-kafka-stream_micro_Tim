package com.course.kafka.broker.stream.feedback.rating;
import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingOneMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;

// video 103                                                               <  insert type , return type  >
public class A_02_FeedbackRatingOneTransformer implements ValueTransformer<FeedbackMessage, FeedbackRatingOneMessage> {
    private ProcessorContext processorContext;
    private final String stateStoreName;              // as we are using final  so it must not be null, so we are initializing
    private KeyValueStore<String, A_01_FeedbackRatingOneStoreValue> ratingStateStore;               //it in the constructor

    public A_02_FeedbackRatingOneTransformer(String stateStoreName) {  //"feedbackRatingOneStateStore"   this is the name we
                                                                       // passed from   A_03_FeedbackRatingOneStream  class
       if(StringUtils.isEmpty(stateStoreName)) { throw new IllegalArgumentException("State store name must not empty----###");  }
       this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.processorContext = context;
     this.ratingStateStore
           = (KeyValueStore<String, A_01_FeedbackRatingOneStoreValue>) this.processorContext.getStateStore(stateStoreName);
    }                 // here we are retrieving from the Stores the FeedbackRatingOneStoreVale  properties  at the time of init()
                      // method call first time then it return null but see down in transform method if it is null then their
                      //we are creating new Object as    "new A_01_FeedbackRatingOneStoreValue()" if this object is already
                      // their with some data then we are fetching the data here at this init() method call
    @Override
    public FeedbackRatingOneMessage transform(FeedbackMessage feedbackMessage) {  // in the FeedbackRatingOne class
                                  //we are having feedbackStream, in this Stream we are consuming topic with datatype
                                  // FeedbackMessage so form their we are getting this as argument

        // as calculation is based on  "location" so we are checking that perviously that location already exists or not
        // if location exists then fetch the old data and add to new if not exists then just create a dummy object for it
        var storeValue = Optional.ofNullable(ratingStateStore.get(feedbackMessage.getLocation()))
                                                            .orElse(new A_01_FeedbackRatingOneStoreValue());

        // update new store
        var newSumRating = storeValue.getSumRating() + feedbackMessage.getRating(); // here we are adding   old + new
        storeValue.setSumRating(newSumRating);       // here we are setting   the added value as new value

        var newCountRating = storeValue.getCountRating() + 1;    //as one more rating is added so we are updating the count
        storeValue.setCountRating(newCountRating);

        // put new Store to state store
        ratingStateStore.put(feedbackMessage.getLocation(), storeValue);   // here we are putting back to Store the newely
                                                                           // updated value
       // build branch rating
        var branchRating = new FeedbackRatingOneMessage();
        branchRating.setLocation(feedbackMessage.getLocation());
        double averageRating = Math.round((double) newSumRating / newCountRating * 10d) / 10d;
        branchRating.setAverageRating(averageRating);          // as we want to return the AverageRating so here we are
                                                             // just add the logic and put the updating rating and
        return branchRating;                               // returning back to the A_03_FeedbackRatingOneStream class
    }                                                      // as we are calling this class object from their

    @Override
    public void close() { }
}
