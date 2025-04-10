package uk.ac.ed.acp.cw2.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.DeliverCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// Spring Boot annotations and web tools

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

// Kafka libraries

// RabbitMQ libraries
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

// JSON handling
import org.json.JSONObject;

// Java utilities
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * KafkaController is a REST API controller used to interact with Apache Kafka for producing
 * and consuming stock symbol events. This class provides endpoints for sending stock symbols
 * to a Kafka topic and retrieving stock symbols from a Kafka topic.
 * <p>
 * It is designed to handle dynamic Kafka configurations based on the runtime environment
 * and supports security configurations such as SASL and JAAS.
 */
@RestController()
@RequestMapping("/kafka")
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");

    public KafkaController(RuntimeEnvironment environment) {
        this.environment = environment;
    }
    private static final String STUDENT_UID = "s2751499";
    /**
     * Constructs Kafka properties required for KafkaProducer and KafkaConsumer configuration.
     *
     * @param environment the runtime environment providing dynamic configuration details
     *                     such as Kafka bootstrap servers.
     * @return a Properties object containing configuration properties for Kafka operations.
     */
    private Properties getKafkaProperties(RuntimeEnvironment environment) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("enable.auto.commit", "true");
        kafkaProps.put("acks", "all");

        kafkaProps.put("group.id", UUID.randomUUID().toString());
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("enable.auto.commit", "true");

        if (environment.getKafkaSecurityProtocol() != null) {
            kafkaProps.put("security.protocol", environment.getKafkaSecurityProtocol());
        }
        if (environment.getKafkaSaslMechanism() != null) {
            kafkaProps.put("sasl.mechanism", environment.getKafkaSaslMechanism());
        }
        if (environment.getKafkaSaslJaasConfig() != null) {
            kafkaProps.put("sasl.jaas.config", environment.getKafkaSaslJaasConfig());
        }

        return kafkaProps;
    }

    @PutMapping("/{writeTopic}/{messageCount}")
    public String MessagesPUT(@PathVariable String writeTopic, @PathVariable int messageCount) {
        logger.info("Writing {} messages to topic {}", messageCount, writeTopic);
        Properties kafkaProps = getKafkaProperties(environment);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
            for (int i = 0; i < messageCount; i++) {
                String message = String.format("{\"uid\":\"%s\",\"counter\":%d}", STUDENT_UID, i);
                producer.send(new ProducerRecord<>(writeTopic, STUDENT_UID, message),
                        (metadata, exception) -> {
                            if (exception != null)
                                logger.error("Error sending message", exception);
                            else
                                logger.info("Produced event to topic {}: offset = {}", writeTopic, metadata.offset());
                        });
            }
        } catch (Exception e) {
            logger.error("Kafka producer error", e);
            throw new RuntimeException("Failed to send Kafka messages!", e);
        }

        return "Kafka messages sent successfully!";
    }

    // GET Endpoint: Reading messages from Kafka
    @GetMapping("/{readTopic}/{timeoutInMsec}")
    public List<String> MessagesGET(@PathVariable String readTopic, @PathVariable int timeoutInMsec) {
        logger.info("Reading messages from topic {}", readTopic);
        Properties kafkaProps = getKafkaProperties(environment);

        List<String> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(readTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeoutInMsec));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("[{}] Key: {}, Value: {}, Partition: {}, Offset: {}",
                        record.topic(), record.key(), record.value(), record.partition(), record.offset());
                result.add(record.value());
            }
        } catch (Exception e) {
            logger.error("Kafka consumer error", e);
            throw new RuntimeException("Failed to read Kafka messages!", e);
        }

        return result;
    }



    @PostMapping("/publish")
    public ResponseEntity<String> publishToKafka(@RequestBody Map<String, Object> request) {
        String topic = (String) request.get("topic");
        Map<String, Object> message = (Map<String, Object>) request.get("message");

        if (topic == null || message == null) {
            return ResponseEntity.badRequest().body("Missing 'topic' or 'message' field");
        }

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
            String messageJson = new ObjectMapper().writeValueAsString(message);
            String key = message.get("key") != null ? message.get("key").toString() : UUID.randomUUID().toString();
            System.out.println(key + ": " + messageJson);


            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, messageJson);
            producer.send(record);
            return ResponseEntity.ok("Message published to Kafka topic: " + topic);
        } catch (Exception e) {
            System.out.println("Encountered error publishing message to Kafka topic: " + topic);
            e.printStackTrace();
            return ResponseEntity.status(500).body("Kafka publish failed: " + e.getMessage());
        }
    }


    private String getAcpStorageServiceUrl() {
        String defaultUrl = "https://acp-storage.azurewebsites.net";
        return System.getenv("ACP_STORAGE_SERVICE") != null
                ? System.getenv("ACP_STORAGE_SERVICE")
                : defaultUrl;
    }
    private String storeInAcpStorage(String jsonData) {
        String url = getAcpStorageServiceUrl() + "/api/v1/blob";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(jsonData, headers);
        RestTemplate restTemplate =  new RestTemplate();
        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
        return response.getBody(); // UUID from ACP Storage
    }

    // Corrected RabbitMQ factory creation
    private ConnectionFactory getRabbitMqFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost()); // fix clearly using your getters
        factory.setPort(environment.getRabbitMqPort());
        return factory;
    }

    @PostMapping("/sendStockSymbols/{symbolTopic}/{symbolCount}")
    public void sendStockSymbols(@PathVariable String symbolTopic, @PathVariable int symbolCount) {
        logger.info(String.format("Writing %d symbols in topic %s", symbolCount, symbolTopic));
        Properties kafkaProps = getKafkaProperties(environment);

        try (var producer = new KafkaProducer<String, String>(kafkaProps)) {
            for (int i = 0; i < symbolCount; i++) {
                final String key = stockSymbols[new Random().nextInt(stockSymbols.length)];
                final String value = String.valueOf(i);

                producer.send(new ProducerRecord<>(symbolTopic, key, value), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        logger.info(String.format("Produced event to topic %s: key = %-10s value = %s%n", symbolTopic, key, value));
                }).get(1000, TimeUnit.MILLISECONDS);
            }
            logger.info(String.format("%d record(s) sent to Kafka\n", symbolCount));
        } catch (ExecutionException e) {
            logger.error("execution exc: " + e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            logger.error("timeout exc: " + e);
        } catch (InterruptedException e) {
            logger.error("interrupted exc: " + e);
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/receiveStockSymbols/{symbolTopic}/{consumeTimeMsec}")
    public List<AbstractMap.SimpleEntry<String, String>> receiveStockSymbols(@PathVariable String symbolTopic, @PathVariable int consumeTimeMsec) {
        logger.info(String.format("Reading stock-symbols from topic %s", symbolTopic));
        Properties kafkaProps = getKafkaProperties(environment);

        var result = new ArrayList<AbstractMap.SimpleEntry<String, String>>();

        try (var consumer = new KafkaConsumer<String, String>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(symbolTopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(consumeTimeMsec));
            for (ConsumerRecord<String, String> record : records) {
                logger.info(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
                result.add(new AbstractMap.SimpleEntry<>(record.key(), record.value()));
            }
        }

        return result;
    }
}
