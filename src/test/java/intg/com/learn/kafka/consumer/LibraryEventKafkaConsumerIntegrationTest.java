package com.learn.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.entity.Book;
import com.learn.kafka.entity.LibraryEvent;
import com.learn.kafka.entity.LibraryEventType;
import com.learn.kafka.jpa.FailureRecordRepository;
import com.learn.kafka.jpa.LibraryEventsRepository;
import com.learn.kafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = { "library-events", "library-events.RETRY", "library-events.DLT" }, partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "retryListener.startup=false"
})
class LibraryEventKafkaConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private FailureRecordRepository failureRecordRepository;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String dltTopic;


    @SpyBean
    private LibraryEventKafkaConsumer libraryEventKafkaConsumerSpy;

    @SpyBean
    private LibraryEventsService libraryEventsServiceSpy;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        MessageListenerContainer container = kafkaListenerEndpointRegistry.getAllListenerContainers().stream()
                .filter(messageListenerContainer -> "library-events-listener-group".equals(messageListenerContainer.getGroupId()))
                .toList().get(0);
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
//        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getAllListenerContainers()) {
//            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
//        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"First\",\"bookAuthor\":\"Me\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        verify(libraryEventKafkaConsumerSpy).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();

        assertThat(libraryEvents).hasSize(1);
        LibraryEvent libraryEvent = libraryEvents.get(0);
        assertThat(libraryEvent).isNotNull();
        assertThat(libraryEvent.getLibraryEventId()).isNotNull();
        assertThat(libraryEvent.getBook().getBookId()).isEqualTo(123);
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"First\",\"bookAuthor\":\"Me\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder()
                .bookId(456)
                .bookName("Second")
                .bookAuthor("You")
                .build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        verify(libraryEventKafkaConsumerSpy).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent updatedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertThat(updatedLibraryEvent).isNotNull();
        assertThat(updatedLibraryEvent.getBook().getBookId()).isEqualTo(456);
    }

    @Test
    @Disabled
    void publishUpdateLibraryEvent_LibraryEventIdIsNull_Fail() throws JsonProcessingException, ExecutionException, InterruptedException {
        final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"First\",\"bookAuthor\":\"Me\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        verify(libraryEventKafkaConsumerSpy).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy).processLibraryEvent(isA(ConsumerRecord.class));

        HashMap<String, Object> configs = new HashMap<>(
                KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(
                configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, dltTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, dltTopic);

        assertThat(json).isEqualTo(consumerRecord.value());
    }

    @Test
    @Disabled
    void publishUpdateLibraryEvent_999_Fail() throws JsonProcessingException, ExecutionException, InterruptedException {
        final String json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"First\",\"bookAuthor\":\"Me\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        verify(libraryEventKafkaConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        HashMap<String, Object> configs = new HashMap<>(
                KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(
                configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);

        assertThat(json).isEqualTo(consumerRecord.value());
    }

    @Test
    void publishUpdateLibraryEvent_LibraryEvent_Failure_Record() throws JsonProcessingException, ExecutionException, InterruptedException {
        final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"First\",\"bookAuthor\":\"Me\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        verify(libraryEventKafkaConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        assertThat(failureRecordRepository.findAll()).hasSize(1);
    }
}
