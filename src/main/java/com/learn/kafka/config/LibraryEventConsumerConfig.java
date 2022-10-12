package com.learn.kafka.config;

import com.learn.kafka.jpa.FailureRecordRepository;
import com.learn.kafka.scheduler.RetryScheduler;
import com.learn.kafka.service.FailureRecordService;
import com.learn.kafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Slf4j
@Configuration
@EnableKafka
@RequiredArgsConstructor
public class LibraryEventConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";
    public static final String SUCCESS = "SUCCESS";

    private final KafkaProperties properties;
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String dltTopic;

    @Bean
    public NewTopic retryTopic() {
        return TopicBuilder
                .name(retryTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic dltTopic() {
        return TopicBuilder
                .name(dltTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        return new TopicPartition(dltTopic, r.partition());
                    }
                });
    }

    @Bean
    public RetryScheduler retryScheduler(
            final FailureRecordRepository failureRecordRepository,
            final LibraryEventsService libraryEventsService
    ) {
        return new RetryScheduler(failureRecordRepository, libraryEventsService);
    }

    @Bean
    public FailureRecordService failureRecordService(
            final FailureRecordRepository failureRecordRepository
    ) {
        return new FailureRecordService(failureRecordRepository);
    }

    @Bean
    public ConsumerRecordRecoverer consumerRecordRecoverer(final FailureRecordService failureRecordService) {
        return ((consumerRecord, e) -> {
            log.error("Exception in ConsumerRecordRecoverer: {}", e.getMessage());
            final ConsumerRecord<Integer, String> record = (ConsumerRecord<Integer, String>) consumerRecord;
            if (e.getCause() instanceof RecoverableDataAccessException) {
                log.info("Inside recovery");
                failureRecordService.saveFailureRecord(record, e, RETRY);
            } else {
                log.info("Inside non-recovery");
                failureRecordService.saveFailureRecord(record, e, DEAD);
            }
        });
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(
            //final DeadLetterPublishingRecoverer deadLetterPublishingRecoverer
            final ConsumerRecordRecoverer consumerRecordRecoverer
    ) {
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);
        ExponentialBackOffWithMaxRetries backOffWithMaxRetries = new ExponentialBackOffWithMaxRetries(2);
        backOffWithMaxRetries.setMaxInterval(1_000L);
        backOffWithMaxRetries.setMultiplier(2.0);
        backOffWithMaxRetries.setMaxInterval(2_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
//                deadLetterPublishingRecoverer,
//                fixedBackOff,
                consumerRecordRecoverer,
                backOffWithMaxRetries
        );
//        List.of(
//                IllegalArgumentException.class
//        ).forEach(errorHandler::addNotRetryableExceptions);
        List.of(
                RecoverableDataAccessException.class
        ).forEach(errorHandler::addRetryableExceptions);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.info("Failed record in retry listener, exception: {}, delivery attempt: {}", ex.getMessage(), deliveryAttempt));
        return errorHandler;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));
        factory.setConcurrency(3);
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
