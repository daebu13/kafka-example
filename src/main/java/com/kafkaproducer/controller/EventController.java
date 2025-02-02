package com.kafkaproducer.controller;

import com.kafkaproducer.dto.Customer;
import com.kafkaproducer.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class EventController {
    @Autowired
    private KafkaMessagePublisher publisher;
    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message)  {
        try {
            for (int i = 0; i<=10000;i++) {
                publisher.sendMessageToTopic(message+ " " + i);
            }

            return ResponseEntity.ok("message published successfully ..");
        }catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

    }
    @PostMapping("/publishEvent")

    public ResponseEntity<?> sendEvents(@RequestBody Customer customer){

        publisher.sendEventToTopic(customer);
        return  ResponseEntity.ok("message published successfully ..");


    }
}
