package com.learn.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LibraryEventRetryConsumer {

    private final LibraryEventsService libraryEventsService;

    @KafkaListener(
            topics = { "${topics.retry}" },
            autoStartup = "${retryListener.startup:true}",
            groupId = "retry-listener-group"
    )
    public void onMessage(final ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Consumer record Retry listener: {}", consumerRecord);
        consumerRecord.headers().forEach(header -> log.info("Header key: {}, value: {}", header.key(), new String(header.value())));
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
