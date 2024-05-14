package com.kafkaproducer.service;

import com.kafkaproducer.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {
    @Autowired
    private KafkaTemplate<String,Object> template;

    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, Object>> future = template.send("payments-5", message);
        //handle results asynchronously
        future.whenComplete((result, error) -> {
            if (error!= null) {


                System.out.println("Error publishing message: " + error.getMessage());
            } else {
                System.out.println("Successfully published message: " + result.getRecordMetadata().offset());
            }
        });
    }


    public void sendEventToTopic(Customer customer){

        try{
            CompletableFuture<SendResult<String, Object>> future = template.send("payments-6", customer.toString());
            future.whenComplete((result,error)->{
                if (error!= null) {
                    System.out.println("Error publishing message: " + error.getMessage());
                } else {
                    System.out.println("Successfully published message: " + customer.toString() + "with offset: " + result.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            System.out.println("ERROR " + e.getMessage());
        }

    }
}
