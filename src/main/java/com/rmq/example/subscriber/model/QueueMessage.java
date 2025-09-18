package com.rmq.example.subscriber.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class QueueMessage {
    @JsonProperty("key1")
    private String key1;

    @JsonProperty("key2")
    private String key2;
}
