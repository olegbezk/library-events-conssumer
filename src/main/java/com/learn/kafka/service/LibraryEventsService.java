package com.learn.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.entity.LibraryEvent;
import com.learn.kafka.jpa.LibraryEventsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(final ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        final LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("LibraryEvent: {}", libraryEvent);

        if (libraryEvent != null && (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 999)) {
            throw new RecoverableDataAccessException("Temporary network issue");
        }

        switch (Objects.requireNonNull(libraryEvent).getLibraryEventType()) {
            case NEW -> save(libraryEvent);
            case UPDATE -> validateAndSave(libraryEvent);
            default -> log.info("Invalid library event type");
        }
    }

    private void validateAndSave(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event ID is missing");
        }

        Optional<LibraryEvent> event = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());

        if (event.isEmpty()) {
            throw new IllegalArgumentException("Library event is missing");
        }

        save(libraryEvent);
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully persisted library event {}", libraryEvent);
    }
}
