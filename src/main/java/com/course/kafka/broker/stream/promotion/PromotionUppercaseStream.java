package com.course.kafka.broker.stream.promotion;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Bean;

//@Configuration
public class PromotionUppercaseStream {

    @Bean
    public KStream<String, String> kstreamPromotionUppercase(StreamsBuilder builder) {
        KStream<String, String> sourceStream =
                builder.stream("t.commodity.promotion", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> uppercaseStream = sourceStream.mapValues(s -> s.toUpperCase());
        uppercaseStream.to("t.commodity.promotion-uppercase");

        // useful for debugging, but don't do this on production
        sourceStream.print(Printed.<String, String>toSysOut().withLabel("Original Stream"));
        uppercaseStream.print(Printed.<String, String>toSysOut().withLabel("Uppercase Stream"));

        return sourceStream;
    }
}
/* Note: easy to understand here we are taking a Topic "t.commodity.promotion" and fetching the data
from it and put it in  "scourceStream22"  after that we are creating a new Stream with same source
and modify the data to uppercase and after modifying the data will be saved into new Stream
and we send this data to another Topic   "uppercaseStream22.to("t.commodity.promotion-uppercase")"

but main point is the original Stream is still unchanged and we should also never change the
source stream it is immutable, and at line 26 we are returning the original Stream back
 */
