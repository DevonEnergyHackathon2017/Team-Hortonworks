package com.hortonworks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.hortonworks.LocationRecord;

import java.util.Date;
import java.util.Random;


@Component
public class GeodataGenerator {

    // This class simulates a person moving around the example pad site.

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private KafkaTemplate kafkaTemplate;

//    double p1Latitude = 32.253870;
//    double p1Longitude = -103.198496;
//    double p2Latitude = 32.257317;
//    double p2Longitude = -103.188127;
//    double extraBuffer = 0.0001;

    private double minLat = 32.253;
    private double maxLat = 32.258;
    private double minLong = -103.198;
    private double maxLong = -103.189;
    private double stepSize = 0.000001;
    private Integer stepDirection = null;
    private Double step = null;

    private LocationRecord getLocationRecord() {
        LocationRecord locationRecord = new LocationRecord();
        locationRecord.setId(1);
        locationRecord.setLatitude((32.253870 + 32.257317) / 2);
        locationRecord.setLongitude((-103.198496 + -103.188127) / 2);
        locationRecord.setTimestamp(new Date().getTime());
        return locationRecord;
    }

    private LocationRecord locationRecord = getLocationRecord();

    @Scheduled(cron = "* * * * * *") // run every second
    private void simulate() throws JsonProcessingException {

        stepDirection = oneOrMinusOne();
        step = stepDirection * stepSize;

        double newLongitude;
        if (locationRecord.getLongitude() + step < minLong | locationRecord.getLongitude() + step > maxLong){
            newLongitude = locationRecord.getLongitude() - step;
        } else {
            newLongitude = locationRecord.getLongitude() + step;
        }

        stepDirection = oneOrMinusOne();
        step = stepDirection * stepSize;

        double newLatitude;
        if (locationRecord.getLatitude() + step < minLat | locationRecord.getLatitude() + step > maxLat){
            newLatitude = locationRecord.getLatitude() - step;
        } else {
            newLatitude = locationRecord.getLatitude() + step;
        }

        locationRecord.setLongitude(newLongitude);
        locationRecord.setLatitude(newLatitude);
        locationRecord.setTimestamp(new Date().getTime());
        logger.info(locationRecord.toString());

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(locationRecord);

        kafkaTemplate.send("transportation", json);

    }

    int oneOrMinusOne(){
        Random random = new Random();
        int randomInt = random.nextInt(2);

        if (randomInt == 0){
            randomInt = -1;
        }

        return randomInt;
    }

}
