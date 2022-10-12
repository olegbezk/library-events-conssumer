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
public class LibraryEventKafkaConsumer {

    private final LibraryEventsService libraryEventsService;

    @KafkaListener(topics = { "${spring.kafka.template.default-topic}" }, groupId = "library-events-listener-group")
    public void onMessage(final ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Consumer record: {}", consumerRecord);
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
