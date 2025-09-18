package com.rmq.example.subscriber.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rmq.example.subscriber.model.QueueMessage;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class SubscriberService {

    @RabbitListener(containerFactory = "rabbitListenerContainerFactory", queues = "${rabbitmq.queuename}")
    public void receiveMessage(Message message) {
        System.out.println("New RabbitMQ Message Received");

        try {
            String messageBody = reqMessageToString(message);
            QueueMessage queueMessage = (QueueMessage) jsonToObject(messageBody, QueueMessage.class);
            System.out.println(queueMessage);
        }catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String reqMessageToString(Message message) {
        return new String(message.getBody(), StandardCharsets.UTF_8);
    }

    private Object jsonToObject(String json, Class<?> classSerializable) {
        try {
            return new ObjectMapper().readValue(json, classSerializable);
        }catch (JsonProcessingException e){
            throw new RuntimeException(e);
        }
    }

}
