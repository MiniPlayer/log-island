/**
 * Copyright 2014 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.kafka.store;

import com.hurence.logisland.kafka.registry.KafkaRegistry;
import com.hurence.logisland.kafka.registry.KafkaRegistryConfig;
import com.hurence.logisland.kafka.serialization.Serializer;
import com.hurence.logisland.kafka.store.exceptions.SerializationException;
import com.hurence.logisland.kafka.store.exceptions.StoreException;
import com.hurence.logisland.kafka.store.exceptions.StoreInitializationException;
import com.hurence.logisland.kafka.store.exceptions.StoreTimeoutException;
import io.confluent.rest.RestConfig;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.common.TopicExistsException;
import kafka.log.LogConfig;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaStore<K, V> implements Store<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaStore.class);

    private final String kafkaClusterZkUrl;
    private final String topic;
    private final int desiredReplicationFactor;
    private final String groupId;
    private final StoreUpdateHandler<K, V> storeUpdateHandler;
    private final Serializer<K, V> serializer;
    private final Store<K, V> localStore;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final int initTimeout;
    private final int timeout;
    private final Seq<Broker> brokerSeq;
    private final String bootstrapBrokers;
    private final ZkUtils zkUtils;
    private KafkaProducer<byte[], byte[]> producer;
    private KafkaStoreReaderThread<K, V> kafkaTopicReader;
    // Noop key is only used to help reliably determine last offset; reader thread ignores
    // messages with this key
    private final K noopKey;
    private volatile long lastWrittenOffset = -1L;
    private final KafkaRegistryConfig config;

    public KafkaStore(String kafkaStoreTopicConfig,
                      KafkaRegistryConfig config,
                      StoreUpdateHandler<K, V> storeUpdateHandler,
                      Serializer<K, V> serializer,
                      Store<K, V> localStore,
                      K noopKey) {
        this.kafkaClusterZkUrl =
                config.getString(KafkaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG);


        this.topic = config.getString(kafkaStoreTopicConfig);

        this.desiredReplicationFactor =
                config.getInt(KafkaRegistryConfig.KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG);
        int port = KafkaRegistry.getPortForIdentity(config.getInt(KafkaRegistryConfig.PORT_CONFIG),
                config.getList(RestConfig.LISTENERS_CONFIG));
        this.groupId = String.format("schema-registry-%s-%d",
                config.getString(KafkaRegistryConfig.HOST_NAME_CONFIG),
                port);
        initTimeout = config.getInt(KafkaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
        timeout = config.getInt(KafkaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
        this.storeUpdateHandler = storeUpdateHandler;
        this.serializer = serializer;
        this.localStore = localStore;
        this.noopKey = noopKey;

        int zkSessionTimeoutMs =
                config.getInt(KafkaRegistryConfig.KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG);
        this.zkUtils = ZkUtils.apply(
                kafkaClusterZkUrl, zkSessionTimeoutMs, zkSessionTimeoutMs,
                JaasUtils.isZkSecurityEnabled());
        this.brokerSeq = zkUtils.getAllBrokersInCluster();

        List<String> bootstrapServersConfig = config.getList(KafkaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG);
        List<String> endpoints;
        if (bootstrapServersConfig.isEmpty()) {
            endpoints = brokersToEndpoints(JavaConversions.seqAsJavaList(this.brokerSeq));
        } else {
            endpoints = bootstrapServersConfig;
        }
        this.bootstrapBrokers = filterBrokerEndpoints(endpoints);
        log.info("Initializing KafkaStore with broker endpoints: " + this.bootstrapBrokers);

        this.config = config;
    }

    @Override
    public void init() throws StoreInitializationException {
        if (initialized.get()) {
            throw new StoreInitializationException(
                    "Illegal state while initializing store. Store was already initialized");
        }

        // create the schema topic if needed
        createSchemaTopic();

        // set the producer properties and initialize a Kafka producer client
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put(ProducerConfig.RETRIES_CONFIG, 0); // Producer should not retry

        addSslConfigsToClientProperties(this.config, props);

        producer = new KafkaProducer<byte[], byte[]>(props);

        // start the background thread that subscribes to the Kafka topic and applies updates.
        // the thread must be created after the schema topic has been created.
        this.kafkaTopicReader =
                new KafkaStoreReaderThread<>(this.bootstrapBrokers, topic, groupId,
                        this.storeUpdateHandler, serializer, this.localStore,
                        this.noopKey, this.config);
        this.kafkaTopicReader.start();

        try {
            waitUntilKafkaReaderReachesLastOffset(initTimeout);
        } catch (StoreException e) {
            throw new StoreInitializationException(e);
        }

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new StoreInitializationException("Illegal state while initializing store. Store "
                    + "was already initialized");
        }
    }

    public static void addSslConfigsToClientProperties(KafkaRegistryConfig config, Properties props) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                config.getString(KafkaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG));
        if (config.getString(KafkaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG).equals(
                KafkaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_SSL)) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG));
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG));
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_TRUSTSTORE_TYPE_CONFIG));
            props.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
            putIfNotEmptyString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG), props);
            putIfNotEmptyString(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG), props);
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_KEYSTORE_TYPE_CONFIG));
            props.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_CONFIG));
            putIfNotEmptyString(SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_KEY_PASSWORD_CONFIG), props);
            putIfNotEmptyString(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_ENABLED_PROTOCOLS_CONFIG), props);
            props.put(SslConfigs.SSL_PROTOCOL_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_PROTOCOL_CONFIG));
            putIfNotEmptyString(SslConfigs.SSL_PROVIDER_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_PROVIDER_CONFIG), props);
            putIfNotEmptyString(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_CIPHER_SUITES_CONFIG), props);
            putIfNotEmptyString(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                    config.getString(KafkaRegistryConfig.KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), props);
        }
    }

    // helper method to only add a property if its not the empty string. This is required
    // because some Kafka client configs expect a null default value, yet ConfigDef doesn't
    // support null default values.
    private static void putIfNotEmptyString(String parameter, String value, Properties props) {
        if (!value.trim().isEmpty()) {
            props.put(parameter, value);
        }
    }

    private void createSchemaTopic() throws StoreInitializationException {
        if (AdminUtils.topicExists(zkUtils, topic)) {
            verifySchemaTopic();
            return;
        }
        int numLiveBrokers = brokerSeq.size();
        if (numLiveBrokers <= 0) {
            throw new StoreInitializationException("No live Kafka brokers");
        }
        int schemaTopicReplicationFactor = Math.min(numLiveBrokers, desiredReplicationFactor);
        if (schemaTopicReplicationFactor < desiredReplicationFactor) {
            log.warn("Creating the schema topic " + topic + " using a replication factor of " +
                    schemaTopicReplicationFactor + ", which is less than the desired one of "
                    + desiredReplicationFactor + ". If this is a production environment, it's " +
                    "crucial to add more brokers and increase the replication factor of the topic.");
        }
        Properties schemaTopicProps = new Properties();
        schemaTopicProps.put(LogConfig.CleanupPolicyProp(), "compact");

        try {
            AdminUtils.createTopic(zkUtils, topic, 1, schemaTopicReplicationFactor, schemaTopicProps,
                    RackAwareMode.Enforced$.MODULE$);
        } catch (TopicExistsException e) {
            // This is ok.
        }
    }

    static List<String> brokersToEndpoints(List<Broker> brokers) {
        List<String> endpoints = new LinkedList<String>();
        for (Broker broker : brokers) {
            for (EndPoint ep : JavaConversions.asJavaCollection(broker.endPoints().values())) {
                endpoints.add(ep.connectionString());
            }
        }
        return endpoints;
    }

    static String filterBrokerEndpoints(List<String> endpoints) {
        StringBuilder sb = new StringBuilder();

        for (String endpoint : endpoints) {
            if (endpoint.startsWith(KafkaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_SSL + "://")
                    || endpoint.startsWith(KafkaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_PLAINTEXT + "://")) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(endpoint);
            } else {
                log.warn("Ignoring non-plaintext and non-SSL Kafka endpoint: " + endpoint);
            }
        }

        if (sb.length() == 0) {
            throw new ConfigException("Only plaintext and SSL Kafka endpoints are supported and " +
                    "none are configured.");
        }

        return sb.toString();
    }

    private void verifySchemaTopic() {
        Set<String> topics = new HashSet<String>();
        topics.add(topic);

        // check # partition and the replication factor
        scala.collection.Map partitionAssignment = zkUtils.getPartitionAssignmentForTopics(
                JavaConversions.asScalaSet(topics).toSeq())
                .get(topic).get();

        if (partitionAssignment.size() != 1) {
            log.warn("The schema topic " + topic + " should have only 1 partition.");
        }

        if (((Seq) partitionAssignment.get(0).get()).size() < desiredReplicationFactor) {
            log.warn("The replication factor of the schema topic " + topic + " is less than the " +
                    "desired one of " + desiredReplicationFactor + ". If this is a production " +
                    "environment, it's crucial to add more brokers and increase the replication " +
                    "factor of the topic.");
        }

        // check the retention policy
        Properties prop = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
        String retentionPolicy = prop.getProperty(LogConfig.CleanupPolicyProp());
        if (retentionPolicy == null || "compact".compareTo(retentionPolicy) != 0) {
            log.warn("The retention policy of the schema topic " + topic + " may be incorrect. " +
                    "Please configure it with compact.");
        }
    }

    /**
     * Wait until the KafkaStore catches up to the last message in the Kafka topic.
     */
    public void waitUntilKafkaReaderReachesLastOffset(int timeoutMs) throws StoreException {
        long offsetOfLastMessage = getLatestOffset(timeoutMs);
        log.info("Wait to catch up until the offset of the last message at " + offsetOfLastMessage);
        kafkaTopicReader.waitUntilOffset(offsetOfLastMessage, timeoutMs, TimeUnit.MILLISECONDS);
        log.debug("Reached offset at " + offsetOfLastMessage);
    }

    public void markLastWrittenOffsetInvalid() {
        lastWrittenOffset = -1L;
    }

    @Override
    public V get(K key) throws StoreException {
        assertInitialized();
        return localStore.get(key);
    }

    @Override
    public void put(K key, V value) throws StoreTimeoutException, StoreException {
        assertInitialized();
        if (key == null) {
            throw new StoreException("Key should not be null");
        }

        // write to the Kafka topic
        ProducerRecord<byte[], byte[]> producerRecord = null;
        try {
            producerRecord =
                    new ProducerRecord<byte[], byte[]>(topic, 0, this.serializer.serializeKey(key),
                            value == null ? null : this.serializer.serializeValue(
                                    value));
        } catch (SerializationException e) {
            throw new StoreException("Error serializing schema while creating the Kafka produce "
                    + "record", e);
        }

        boolean knownSuccessfulWrite = false;
        try {
            log.trace("Sending record to KafkaStore topic: " + producerRecord);
            Future<RecordMetadata> ack = producer.send(producerRecord);
            RecordMetadata recordMetadata = ack.get(timeout, TimeUnit.MILLISECONDS);

            log.trace("Waiting for the local store to catch up to offset " + recordMetadata.offset());
            this.lastWrittenOffset = recordMetadata.offset();
            kafkaTopicReader.waitUntilOffset(this.lastWrittenOffset, timeout, TimeUnit.MILLISECONDS);
            knownSuccessfulWrite = true;
        } catch (InterruptedException e) {
            throw new StoreException("Put operation interrupted while waiting for an ack from Kafka", e);
        } catch (ExecutionException e) {
            throw new StoreException("Put operation failed while waiting for an ack from Kafka", e);
        } catch (TimeoutException e) {
            throw new StoreTimeoutException(
                    "Put operation timed out while waiting for an ack from Kafka", e);
        } catch (KafkaException ke) {
            throw new StoreException("Put operation to Kafka failed", ke);
        } finally {
            if (!knownSuccessfulWrite) {
                this.lastWrittenOffset = -1L;
            }
        }
    }

    @Override
    public Iterator<V> getAll(K key1, K key2) throws StoreException {
        assertInitialized();
        return localStore.getAll(key1, key2);
    }

    @Override
    public void putAll(Map<K, V> entries) throws StoreException {
        assertInitialized();
        // TODO: write to the Kafka topic as a batch
        for (Map.Entry<K, V> entry : entries.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void delete(K key) throws StoreException {
        assertInitialized();
        // delete from the Kafka topic by writing a null value for the key
        put(key, null);
    }

    @Override
    public Iterator<K> getAllKeys() throws StoreException {
        return localStore.getAllKeys();
    }

    @Override
    public void close() {
        kafkaTopicReader.shutdown();
        log.debug("Kafka store reader thread shut down");
        producer.close();
        log.debug("Kafka store producer shut down");
        zkUtils.close();
        log.debug("Kafka store zookeeper client shut down");
        localStore.close();
        log.debug("Kafka store shut down complete");
    }

    /**
     * For testing.
     */
    KafkaStoreReaderThread<K, V> getKafkaStoreReaderThread() {
        return this.kafkaTopicReader;
    }

    private void assertInitialized() throws StoreException {
        if (!initialized.get()) {
            throw new StoreException("Illegal state. Store not initialized yet");
        }
    }

    /**
     * Return the latest offset of the store topic.
     * <p>
     * The most reliable way to do so in face of potential Kafka broker failure is to produce
     * successfully to the Kafka topic and get the offset of the returned metadata.
     * <p>
     * If the most recent write to Kafka was successful (signaled by lastWrittenOffset >= 0),
     * immediately return that offset. Otherwise write a "Noop key" to Kafka in order to find the
     * latest offset.
     */
    private long getLatestOffset(int timeoutMs) throws StoreException {
        ProducerRecord<byte[], byte[]> producerRecord = null;

        if (this.lastWrittenOffset >= 0) {
            return this.lastWrittenOffset;
        }

        try {
            producerRecord =
                    new ProducerRecord<byte[], byte[]>(topic, 0, this.serializer.serializeKey(noopKey), null);
        } catch (SerializationException e) {
            throw new StoreException("Failed to serialize noop key.", e);
        }

        try {
            log.trace("Sending Noop record to KafkaStore to find last offset.");
            Future<RecordMetadata> ack = producer.send(producerRecord);
            RecordMetadata metadata = ack.get(timeoutMs, TimeUnit.MILLISECONDS);
            this.lastWrittenOffset = metadata.offset();
            log.trace("Noop record's offset is " + this.lastWrittenOffset);
            return this.lastWrittenOffset;
        } catch (Exception e) {
            throw new StoreException("Failed to write Noop record to kafka store.", e);
        }
    }
}
