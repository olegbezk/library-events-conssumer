package com.learn.kafka.service;

import com.learn.kafka.entity.FailureRecord;
import com.learn.kafka.jpa.FailureRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
@RequiredArgsConstructor
public class FailureRecordService {

    private final FailureRecordRepository failureRecordRepository;

    public void saveFailureRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, String status) {
        FailureRecord failureRecord = FailureRecord.builder()
                .errorRecord(consumerRecord.value())
                .partition(consumerRecord.partition())
                .exception(e.getMessage())
                .topic(consumerRecord.topic())
                .messageKey(consumerRecord.key())
                .offsetValue(consumerRecord.offset())
                .status(status)
                .build();

        failureRecordRepository.save(failureRecord);
    }
}
