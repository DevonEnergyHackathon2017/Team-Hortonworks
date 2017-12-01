package com.hortonworks;

import com.satori.rtm.*;
import com.satori.rtm.model.AnyJson;
import com.satori.rtm.model.SubscriptionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class SatoriRepublisher {

    // We initially wanted a stream of vehicle movement data and so we used sample transportation data from Satori.

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${satori.endpoint}")
    private String satoriEndpoint;

    @Value("${satori.appkey}")
    private String satoriAppkey;

    @Value("${satori.channel}")
    private String satoriChannel;

    @Autowired
    private KafkaTemplate kafkaTemplate;

//    @PostConstruct
    private void satoriTransportationRepublisher(){

        final RtmClient client = new RtmClientBuilder(satoriEndpoint, satoriAppkey)
                .setListener(new RtmClientAdapter() {
                    @Override
                    public void onEnterConnected(RtmClient client) {
                        logger.info("Connected to Satori RTM");
                    }
                })
                .build();

        SubscriptionAdapter listener = new SubscriptionAdapter() {
            @Override
            public void onSubscriptionData(SubscriptionData data) {
                for (AnyJson json : data.getMessages()) {
                    kafkaTemplate.send("transportation", json.toString());
                }
            }
        };

        client.createSubscription(satoriChannel, SubscriptionMode.SIMPLE, listener);

        client.start();
    }

}