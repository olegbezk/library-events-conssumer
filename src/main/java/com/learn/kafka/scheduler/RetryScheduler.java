package com.learn.kafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafka.config.LibraryEventConsumerConfig;
import com.learn.kafka.entity.FailureRecord;
import com.learn.kafka.jpa.FailureRecordRepository;
import com.learn.kafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
@RequiredArgsConstructor
public class RetryScheduler {

    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventsService libraryEventsService;

    @Scheduled(cron = "*/100 * * * * *")
    public void retryFailedRecords() {
        log.info("Retry scheduling started!");
        failureRecordRepository.findAllByStatus(LibraryEventConsumerConfig.RETRY)
                .forEach(failureRecord -> {
                    log.info("Retrying failure record: {}", failureRecord);
                    ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(LibraryEventConsumerConfig.SUCCESS);
                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecords: {}", e.getMessage());
                    }
                });
        log.info("Retry scheduling ended!");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(final FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffsetValue(),
                failureRecord.getMessageKey(),
                failureRecord.getErrorRecord()
        );
    }
}
