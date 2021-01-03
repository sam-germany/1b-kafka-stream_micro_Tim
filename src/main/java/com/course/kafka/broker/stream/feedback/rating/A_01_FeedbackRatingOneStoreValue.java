package com.course.kafka.broker.stream.feedback.rating;

public class A_01_FeedbackRatingOneStoreValue {

    private long countRating;
    private long sumRating;









    public long getCountRating() {
        return countRating;
    }

    public void setCountRating(long countRating) {
        this.countRating = countRating;
    }

    public long getSumRating() {
        return sumRating;
    }

    public void setSumRating(long sumRating) {
        this.sumRating = sumRating;
    }

    @Override
    public String toString() {
        return "A_01_FeedbackRating{" +
                "countRating=" + countRating +
                ", sumRating=" + sumRating +
                '}';
    }
}
