# Kafka Java API Complete List (Version 4.2.0)

Reference: https://kafka.apache.org/42/javadoc/

---

## Table of Contents

1. [Producer API](#1-producer-api)
2. [Consumer API](#2-consumer-api)
3. [Admin API](#3-admin-api)
4. [Common API](#4-common-api)
5. [Streams API](#5-streams-api)
6. [Connect API](#6-connect-api)
7. [Method Count Statistics](#7-method-count-statistics)

---

## 1. Producer API (org.apache.kafka.clients.producer)

### 1.1 KafkaProducer<K,V>

**Description**: A Kafka client that publishes records to the Kafka cluster. Thread-safe.

**Stability**: @Stable

#### Constructors (4 methods)

| Method | Signature | Description |
|--------|-----------|-------------|
| KafkaProducer | `KafkaProducer(Map<String,Object> configs)` | Instantiate with Map configuration |
| KafkaProducer | `KafkaProducer(Map<String,Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer)` | Instantiate with Map and custom serializers |
| KafkaProducer | `KafkaProducer(Properties properties)` | Instantiate with Properties |
| KafkaProducer | `KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer)` | Instantiate with Properties and custom serializers |

#### Public Methods (15 methods)

| Method | Return Type | Signature | Description | Stability |
|--------|-------------|-----------|-------------|-----------|
| initTransactions | void | `initTransactions()` | Initialize transactions. Must be called first when transactional.id is set | @Stable |
| beginTransaction | void | `beginTransaction()` | Start a new transaction | @Stable |
| sendOffsetsToTransaction | void | `sendOffsetsToTransaction(Map<TopicPartition,OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata)` | Send offsets to consumer group coordinator as part of transaction | @Stable |
| commitTransaction | void | `commitTransaction()` | Commit the ongoing transaction | @Stable |
| abortTransaction | void | `abortTransaction()` | Abort the ongoing transaction | @Stable |
| send | Future<RecordMetadata> | `send(ProducerRecord<K,V> record)` | Asynchronously send a record to a topic | @Stable |
| send | Future<RecordMetadata> | `send(ProducerRecord<K,V> record, Callback callback)` | Asynchronously send with callback | @Stable |
| flush | void | `flush()` | Make all buffered records available to send and block on completion | @Stable |
| partitionsFor | List<PartitionInfo> | `partitionsFor(String topic)` | Get partition metadata for a topic | @Stable |
| metrics | Map<MetricName,? extends Metric> | `metrics()` | Get full set of internal metrics | @Stable |
| registerMetricForSubscription | void | `registerMetricForSubscription(KafkaMetric metric)` | Add application metric for subscription | @Evolving |
| unregisterMetricFromSubscription | void | `unregisterMetricFromSubscription(KafkaMetric metric)` | Remove application metric from subscription | @Evolving |
| clientInstanceId | Uuid | `clientInstanceId(Duration timeout)` | Get client instance ID for telemetry | @Evolving |
| close | void | `close()` | Close the producer | @Stable |
| close | void | `close(Duration timeout)` | Close with timeout | @Stable |

#### Fields (2 constants)

| Field | Type | Description |
|-------|------|-------------|
| NETWORK_THREAD_PREFIX | String | Network thread prefix constant |
| PRODUCER_METRIC_GROUP_NAME | String | Producer metric group name |

---

### 1.2 ProducerRecord<K,V>

**Description**: A key/value pair to be sent to Kafka.

**Stability**: @Stable

#### Constructors (6 methods)

| Method | Signature | Description |
|--------|-----------|-------------|
| ProducerRecord | `ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers)` | Full constructor with headers |
| ProducerRecord | `ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value)` | Constructor with timestamp |
| ProducerRecord | `ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers)` | Constructor with partition and headers |
| ProducerRecord | `ProducerRecord(String topic, Integer partition, K key, V value)` | Constructor with partition |
| ProducerRecord | `ProducerRecord(String topic, K key, V value)` | Basic constructor |
| ProducerRecord | `ProducerRecord(String topic, V value)` | Constructor with no key |

#### Public Methods (9 methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| topic | String | Get topic name |
| headers | Headers | Get headers |
| key | K | Get key (may be null) |
| value | V | Get value |
| timestamp | Long | Get timestamp in milliseconds since epoch |
| partition | Integer | Get partition (may be null) |
| toString | String | String representation |
| equals | boolean | Equality check |
| hashCode | int | Hash code |

---

### 1.3 RecordMetadata

**Description**: Metadata for a record that has been acknowledged by the server.

**Stability**: @Stable

#### Public Methods (8 methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| topic | String | Get topic name |
| partition | int | Get partition |
| offset | long | Get offset |
| timestamp | long | Get timestamp |
| serializedKeySize | int | Get serialized key size in bytes |
| serializedValueSize | int | Get serialized value size in bytes |
| hasOffset | boolean | Check if offset is valid |
| hasTimestamp | boolean | Check if timestamp is valid |

---

### 1.4 Callback

**Description**: Callback interface for asynchronous operations.

**Stability**: @Stable

#### Public Methods (1 method)

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| onCompletion | void | `onCompletion(RecordMetadata metadata, Exception exception)` | Called when record is acknowledged |

---

### 1.5 Producer<K,V> (Interface)

**Description**: Producer interface defining common operations.

**Stability**: @Stable

#### Public Methods (same as KafkaProducer)

---

## 2. Consumer API (org.apache.kafka.clients.consumer)

### 2.1 KafkaConsumer<K,V>

**Description**: A client that consumes records from a Kafka cluster. NOT thread-safe.

**Stability**: @Stable

#### Constructors (4 methods)

| Method | Signature | Description |
|--------|-----------|-------------|
| KafkaConsumer | `KafkaConsumer(Map<String,Object> configs)` | Instantiate with Map |
| KafkaConsumer | `KafkaConsumer(Map<String,Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer)` | Instantiate with Map and deserializers |
| KafkaConsumer | `KafkaConsumer(Properties properties)` | Instantiate with Properties |
| KafkaConsumer | `KafkaConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer)` | Instantiate with Properties and deserializers |

#### Public Methods (52 methods)

| Method | Return Type | Signature | Description | Stability |
|--------|-------------|-----------|-------------|-----------|
| assignment | Set<TopicPartition> | `assignment()` | Get currently assigned partitions | @Stable |
| subscription | Set<String> | `subscription()` | Get subscribed topics | @Stable |
| subscribe | void | `subscribe(Collection<String> topics)` | Subscribe to topics | @Stable |
| subscribe | void | `subscribe(Collection<String> topics, ConsumerRebalanceListener listener)` | Subscribe with rebalance listener | @Stable |
| subscribe | void | `subscribe(Pattern pattern)` | Subscribe using regex pattern | @Stable |
| subscribe | void | `subscribe(Pattern pattern, ConsumerRebalanceListener listener)` | Subscribe pattern with listener | @Stable |
| subscribe | void | `subscribe(SubscriptionPattern pattern)` | Subscribe with SubscriptionPattern | @Evolving |
| subscribe | void | `subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener listener)` | Subscribe pattern with listener | @Evolving |
| unsubscribe | void | `unsubscribe()` | Unsubscribe from all topics | @Stable |
| assign | void | `assign(Collection<TopicPartition> partitions)` | Manually assign partitions | @Stable |
| poll | ConsumerRecords<K,V> | `poll(Duration timeout)` | Poll for new records | @Stable |
| commitSync | void | `commitSync()` | Commit offsets synchronously | @Stable |
| commitSync | void | `commitSync(Duration timeout)` | Commit with timeout | @Stable |
| commitSync | void | `commitSync(Map<TopicPartition,OffsetAndMetadata> offsets)` | Commit specific offsets | @Stable |
| commitSync | void | `commitSync(Map<TopicPartition,OffsetAndMetadata> offsets, Duration timeout)` | Commit offsets with timeout | @Stable |
| commitAsync | void | `commitAsync()` | Commit asynchronously | @Stable |
| commitAsync | void | `commitAsync(OffsetCommitCallback callback)` | Commit async with callback | @Stable |
| commitAsync | void | `commitAsync(Map<TopicPartition,OffsetAndMetadata> offsets, OffsetCommitCallback callback)` | Commit specific offsets async | @Stable |
| seek | void | `seek(TopicPartition partition, long offset)` | Seek to specific offset | @Stable |
| seek | void | `seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata)` | Seek with OffsetAndMetadata | @Stable |
| seekToBeginning | void | `seekToBeginning(Collection<TopicPartition> partitions)` | Seek to beginning | @Stable |
| seekToEnd | void | `seekToEnd(Collection<TopicPartition> partitions)` | Seek to end | @Stable |
| position | long | `position(TopicPartition partition)` | Get current position | @Stable |
| position | long | `position(TopicPartition partition, Duration timeout)` | Get position with timeout | @Stable |
| committed | Map<TopicPartition,OffsetAndMetadata> | `committed(Set<TopicPartition> partitions)` | Get committed offsets | @Stable |
| committed | Map<TopicPartition,OffsetAndMetadata> | `committed(Set<TopicPartition> partitions, Duration timeout)` | Get committed with timeout | @Stable |
| clientInstanceId | Uuid | `clientInstanceId(Duration timeout)` | Get client instance ID | @Evolving |
| metrics | Map<MetricName,? extends Metric> | `metrics()` | Get metrics | @Stable |
| partitionsFor | List<PartitionInfo> | `partitionsFor(String topic)` | Get partition info | @Stable |
| partitionsFor | List<PartitionInfo> | `partitionsFor(String topic, Duration timeout)` | Get partition info with timeout | @Stable |
| listTopics | Map<String,List<PartitionInfo>> | `listTopics()` | List all topics | @Stable |
| listTopics | Map<String,List<PartitionInfo>> | `listTopics(Duration timeout)` | List topics with timeout | @Stable |
| pause | void | `pause(Collection<TopicPartition> partitions)` | Pause partitions | @Stable |
| resume | void | `resume(Collection<TopicPartition> partitions)` | Resume partitions | @Stable |
| paused | Set<TopicPartition> | `paused()` | Get paused partitions | @Stable |
| offsetsForTimes | Map<TopicPartition,OffsetAndTimestamp> | `offsetsForTimes(Map<TopicPartition,Long> timestampsToSearch)` | Get offsets by timestamp | @Stable |
| offsetsForTimes | Map<TopicPartition,OffsetAndTimestamp> | `offsetsForTimes(Map<TopicPartition,Long> timestampsToSearch, Duration timeout)` | Get offsets by timestamp with timeout | @Stable |
| beginningOffsets | Map<TopicPartition,Long> | `beginningOffsets(Collection<TopicPartition> partitions)` | Get beginning offsets | @Stable |
| beginningOffsets | Map<TopicPartition,Long> | `beginningOffsets(Collection<TopicPartition> partitions, Duration timeout)` | Get beginning offsets with timeout | @Stable |
| endOffsets | Map<TopicPartition,Long> | `endOffsets(Collection<TopicPartition> partitions)` | Get end offsets | @Stable |
| endOffsets | Map<TopicPartition,Long> | `endOffsets(Collection<TopicPartition> partitions, Duration timeout)` | Get end offsets with timeout | @Stable |
| currentLag | long | `currentLag(TopicPartition partition)` | Get current lag | @Evolving |
| groupMetadata | ConsumerGroupMetadata | `groupMetadata()` | Get consumer group metadata | @Stable |
| enforceRebalance | void | `enforceRebalance()` | Force rebalance | @Stable |
| enforceRebalance | void | `enforceRebalance(String reason)` | Force rebalance with reason | @Stable |
| close | void | `close()` | Close consumer | @Stable |
| close | void | `close(Duration timeout)` | Close with timeout | @Stable |
| close | void | `close(CloseOptions options)` | Close with options | @Evolving |
| wakeup | void | `wakeup()` | Wakeup consumer from blocking operation | @Stable |
| registerMetricForSubscription | void | `registerMetricForSubscription(KafkaMetric metric)` | Register metric | @Evolving |
| unregisterMetricFromSubscription | void | `unregisterMetricFromSubscription(KafkaMetric metric)` | Unregister metric | @Evolving |

---

### 2.2 ConsumerRecord<K,V>

**Description**: A key/value pair received from Kafka.

**Stability**: @Stable

**Thread Safety**: NOT thread-safe

#### Constants (2 fields)

| Field | Type | Value | Description |
|-------|------|-------|-------------|
| NO_TIMESTAMP | long | -1 | Indicates no timestamp |
| NULL_SIZE | int | -1 | Indicates null size |

#### Constructors (3 methods)

| Method | Signature | Description |
|--------|-----------|-------------|
| ConsumerRecord | `ConsumerRecord(String topic, int partition, long offset, K key, V value)` | Basic constructor |
| ConsumerRecord | `ConsumerRecord(String topic, int partition, long offset, long timestamp, TimestampType timestampType, int serializedKeySize, int serializedValueSize, K key, V value, Headers headers, Optional<Integer> leaderEpoch)` | Full constructor |
| ConsumerRecord | `ConsumerRecord(..., Optional<Short> deliveryCount)` | Full constructor with delivery count |

#### Public Methods (13 methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| topic | String | Get topic name |
| partition | int | Get partition number |
| headers | Headers | Get headers |
| key | K | Get key |
| value | V | Get value |
| offset | long | Get offset position |
| timestamp | long | Get timestamp in milliseconds |
| timestampType | TimestampType | Get timestamp type |
| serializedKeySize | int | Get serialized key size |
| serializedValueSize | int | Get serialized value size |
| leaderEpoch | Optional<Integer> | Get leader epoch |
| deliveryCount | Optional<Short> | Get delivery count |
| toString | String | String representation |

---

### 2.3 ConsumerRecords<K,V>

**Description**: A container for ConsumerRecord objects.

**Stability**: @Stable

#### Public Methods (8 methods)

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| records | List<ConsumerRecord<K,V>> | `records(TopicPartition partition)` | Get records for partition |
| records | Iterable<ConsumerRecord<K,V>> | `records(String topic)` | Get records for topic |
| count | int | `count()` | Total number of records |
| iterator | Iterator<ConsumerRecord<K,V>> | `iterator()` | Iterator over all records |
| partitions | Set<TopicPartition> | `partitions()` | Get partitions with data |
| isEmpty | boolean | `isEmpty()` | Check if empty |
| nextOffsets | Map<TopicPartition,OffsetAndMetadata> | `nextOffsets()` | Get next offsets to process |

---

### 2.4 ConsumerRebalanceListener (Interface)

**Description**: Listener for consumer rebalance events.

**Stability**: @Stable

#### Public Methods (2 methods)

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| onPartitionsRevoked | void | `onPartitionsRevoked(Collection<TopicPartition> partitions)` | Called when partitions revoked |
| onPartitionsAssigned | void | `onPartitionsAssigned(Collection<TopicPartition> partitions)` | Called when partitions assigned |

---

### 2.5 OffsetAndMetadata

**Description**: Offset and metadata for committing.

**Stability**: @Stable

#### Constructors (2 methods)

| Method | Signature | Description |
|--------|-----------|-------------|
| OffsetAndMetadata | `OffsetAndMetadata(long offset)` | Constructor with offset only |
| OffsetAndMetadata | `OffsetAndMetadata(long offset, String metadata)` | Constructor with metadata |
| OffsetAndMetadata | `OffsetAndMetadata(long offset, Optional<Integer> leaderEpoch, String metadata)` | Constructor with leader epoch |

#### Public Methods (3 methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| offset | long | Get offset |
| metadata | String | Get metadata |
| leaderEpoch | Optional<Integer> | Get leader epoch |

---

### 2.6 ConsumerGroupMetadata

**Description**: Metadata for consumer group.

**Stability**: @Stable

#### Public Methods (4 methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| groupId | String | Get group ID |
| memberId | String | Get member ID |
| generationId | int | Get generation ID |
| memberEpoch | int | Get member epoch |

---

## 3. Admin API (org.apache.kafka.clients.admin)

### 3.1 Admin (Interface)

**Description**: Administrative client for Kafka management operations.

**Stability**: @Stable

**Thread Safety**: Thread-safe

#### Static Methods (2 methods)

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| create | Admin | `create(Properties props)` | Create Admin client |
| create | Admin | `create(Map<String,Object> props)` | Create Admin client |

#### Instance Methods (105+ methods - categorized below)

**Topic Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| createTopics | CreateTopicsResult | Create topics |
| deleteTopics | DeleteTopicsResult | Delete topics |
| listTopics | ListTopicsResult | List all topics |
| describeTopics | DescribeTopicsResult | Describe topics |

**ACL Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| describeAcls | DescribeAclsResult | Describe ACLs |
| createAcls | CreateAclsResult | Create ACLs |
| deleteAcls | DeleteAclsResult | Delete ACLs |

**Configuration Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| describeConfigs | DescribeConfigsResult | Describe configs |
| incrementalAlterConfigs | AlterConfigsResult | Incrementally alter configs |
| alterReplicaLogDirs | AlterReplicaLogDirsResult | Alter replica log directories |
| describeLogDirs | DescribeLogDirsResult | Describe log directories |

**Cluster Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| describeCluster | DescribeClusterResult | Describe cluster |
| electLeaders | ElectLeadersResult | Elect leaders |
| alterPartitionReassignments | AlterPartitionReassignmentsResult | Alter partition reassignments |
| listPartitionReassignments | ListPartitionReassignmentsResult | List partition reassignments |

**Consumer Group Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| listConsumerGroups | ListConsumerGroupsResult | List consumer groups |
| describeConsumerGroups | DescribeConsumerGroupsResult | Describe consumer groups |
| deleteConsumerGroups | DeleteConsumerGroupsResult | Delete consumer groups |
| listConsumerGroupOffsets | ListConsumerGroupOffsetsResult | List consumer group offsets |
| alterConsumerGroupOffsets | AlterConsumerGroupOffsetsResult | Alter consumer group offsets |
| deleteConsumerGroupOffsets | DeleteConsumerGroupOffsetsResult | Delete consumer group offsets |
| removeMembersFromConsumerGroup | RemoveMembersFromConsumerGroupResult | Remove members from group |

**Streams Group Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| listStreamsGroupOffsets | ListStreamsGroupOffsetsResult | List streams group offsets |
| alterStreamsGroupOffsets | AlterStreamsGroupOffsetsResult | Alter streams group offsets |
| deleteStreamsGroups | DeleteStreamsGroupsResult | Delete streams groups |
| deleteStreamsGroupOffsets | DeleteStreamsGroupOffsetsResult | Delete streams group offsets |
| describeStreamsGroups | DescribeStreamsGroupsResult | Describe streams groups |

**Share Group Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| describeShareGroups | DescribeShareGroupsResult | Describe share groups |
| alterShareGroupOffsets | AlterShareGroupOffsetsResult | Alter share group offsets |
| listShareGroupOffsets | ListShareGroupOffsetsResult | List share group offsets |
| deleteShareGroupOffsets | DeleteShareGroupOffsetsResult | Delete share group offsets |
| deleteShareGroups | DeleteShareGroupsResult | Delete share groups |

**Delegation Token Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| createDelegationToken | CreateDelegationTokenResult | Create delegation token |
| renewDelegationToken | RenewDelegationTokenResult | Renew delegation token |
| expireDelegationToken | ExpireDelegationTokenResult | Expire delegation token |
| describeDelegationToken | DescribeDelegationTokenResult | Describe delegation tokens |

**Producer/Transaction Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| describeProducers | DescribeProducersResult | Describe producers |
| describeTransactions | DescribeTransactionsResult | Describe transactions |
| abortTransaction | AbortTransactionResult | Abort transaction |
| listTransactions | ListTransactionsResult | List transactions |
| fenceProducers | FenceProducersResult | Fence producers |
| forceTerminateTransaction | TerminateTransactionResult | Force terminate transaction |

**User SCRAM Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| describeUserScramCredentials | DescribeUserScramCredentialsResult | Describe SCRAM credentials |
| alterUserScramCredentials | AlterUserScramCredentialsResult | Alter SCRAM credentials |

**Feature Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| describeFeatures | DescribeFeaturesResult | Describe features |
| updateFeatures | UpdateFeaturesResult | Update features |

**Quota Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| describeClientQuotas | DescribeClientQuotasResult | Describe client quotas |
| alterClientQuotas | AlterClientQuotasResult | Alter client quotas |

**Raft/KRaft Management**

| Method | Return Type | Description |
|--------|-------------|-------------|
| describeMetadataQuorum | DescribeMetadataQuorumResult | Describe metadata quorum |
| addRaftVoter | AddRaftVoterResult | Add Raft voter |
| removeRaftVoter | RemoveRaftVoterResult | Remove Raft voter |
| unregisterBroker | UnregisterBrokerResult | Unregister broker |

**Other Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| listGroups | ListGroupsResult | List all groups |
| listOffsets | ListOffsetsResult | List offsets |
| createPartitions | CreatePartitionsResult | Create partitions |
| deleteRecords | DeleteRecordsResult | Delete records |
| describeClassicGroups | DescribeClassicGroupsResult | Describe classic groups |
| listConfigResources | ListConfigResourcesResult | List config resources |
| listClientMetricsResources | ListClientMetricsResourcesResult | List client metrics resources |
| clientInstanceId | Uuid | Get client instance ID |
| metrics | Map<MetricName,Metric> | Get metrics |
| close | void | Close Admin client |
| close | void | Close with timeout |

---

### 3.2 AdminClient (Class)

**Description**: Concrete implementation of Admin interface.

**Stability**: @Stable

Methods: Same as Admin interface

---

### 3.3 Result Classes

All Admin operations return specialized Result classes with the pattern:

| Result Class | Methods | Description |
|--------------|---------|-------------|
| CreateTopicsResult | `all()`, `values()` | Result for topic creation |
| DeleteTopicsResult | `all()`, `values()` | Result for topic deletion |
| ListTopicsResult | `names()`, `listings()` | Result for topic listing |
| DescribeTopicsResult | `all()`, `topicDescriptions()` | Result for topic description |
| DescribeClusterResult | `nodes()`, `controller()`, `clusterId()` | Result for cluster description |
| DescribeConfigsResult | `all()`, `values()` | Result for config description |
| CreateAclsResult | `all()`, `values()` | Result for ACL creation |
| DeleteAclsResult | `all()`, `values()` | Result for ACL deletion |
| DescribeAclsResult | `values()` | Result for ACL description |
| ... | ... | ... |

---

### 3.4 Option Classes

Admin operations accept Option classes for configuration:

| Option Class | Description |
|--------------|-------------|
| CreateTopicsOptions | Options for creating topics |
| DeleteTopicsOptions | Options for deleting topics |
| ListTopicsOptions | Options for listing topics |
| DescribeTopicsOptions | Options for describing topics |
| DescribeClusterOptions | Options for cluster description |
| DescribeConfigsOptions | Options for config description |
| CreateAclsOptions | Options for creating ACLs |
| DeleteAclsOptions | Options for deleting ACLs |
| DescribeAclsOptions | Options for ACL description |
| AlterConfigsOptions | Options for altering configs |
| ... | ... |

---

## 4. Common API (org.apache.kafka.common)

### 4.1 TopicPartition

**Description**: Represents a topic and partition combination.

**Stability**: @Stable

#### Constructors (2 methods)

| Method | Signature | Description |
|--------|-----------|-------------|
| TopicPartition | `TopicPartition(String topic, int partition)` | Constructor |

#### Public Methods (5 methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| topic | String | Get topic name |
| partition | int | Get partition number |
| hashCode | int | Hash code |
| equals | boolean | Equality check |
| toString | String | String representation |

---

### 4.2 PartitionInfo

**Description**: Information about a topic partition.

**Stability**: @Stable

#### Constructors (2 methods)

| Method | Signature | Description |
|--------|-----------|-------------|
| PartitionInfo | `PartitionInfo(String topic, int partition, Node leader, Node[] replicas, Node[] inSyncReplicas, Node[] offlineReplicas)` | Full constructor |

#### Public Methods (6 methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| topic | String | Get topic |
| partition | int | Get partition |
| leader | Node | Get leader node |
| replicas | Node[] | Get replicas |
| inSyncReplicas | Node[] | Get ISR |
| offlineReplicas | Node[] | Get offline replicas |

---

### 4.3 Node

**Description**: Represents a Kafka broker node.

**Stability**: @Stable

#### Public Methods (7 methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| id | int | Get node ID |
| idString | String | Get node ID as string |
| host | String | Get host |
| port | int | Get port |
| hasRack | boolean | Check if rack is defined |
| rack | String | Get rack |
| isEmpty | boolean | Check if empty |

---

### 4.4 Cluster

**Description**: Represents the Kafka cluster metadata.

**Stability**: @Stable

#### Public Methods (11 methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| nodes | Collection<Node> | Get all nodes |
| nodeById | Node | Get node by ID |
| leaderFor | Node | Get leader for partition |
| partitionCountForTopic | int | Get partition count |
| partitionsForTopic | List<PartitionInfo> | Get partitions for topic |
| availablePartitionsForTopic | List<PartitionInfo> | Get available partitions |
| topics | Set<String> | Get all topics |
| clusterResource | ClusterResource | Get cluster resource |
| partitions | Set<TopicPartition> | Get all partitions |
| byTopicPartition | Map<TopicPartition,PartitionInfo> | Get partition info map |
| controller | Node | Get controller node |

---

### 4.5 Serialization Interfaces

#### Serializer<T> (Interface)

**Description**: Interface for serializing objects to bytes.

**Stability**: @Stable

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| configure | void | `configure(Map<String,?> configs, boolean isKey)` | Configure serializer |
| serialize | byte[] | `serialize(String topic, T data)` | Serialize data |
| serialize | byte[] | `serialize(String topic, Headers headers, T data)` | Serialize with headers |
| close | void | `close()` | Close serializer |

---

#### Deserializer<T> (Interface)

**Description**: Interface for deserializing bytes to objects.

**Stability**: @Stable

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| configure | void | `configure(Map<String,?> configs, boolean isKey)` | Configure deserializer |
| deserialize | T | `deserialize(String topic, byte[] data)` | Deserialize data |
| deserialize | T | `deserialize(String topic, Headers headers, byte[] data)` | Deserialize with headers |
| close | void | `close()` | Close deserializer |

---

#### Serde<T> (Interface)

**Description**: Combined serializer and deserializer.

**Stability**: @Stable

| Method | Return Type | Description |
|--------|-------------|-------------|
| serializer | Serializer<T> | Get serializer |
| deserializer | Deserializer<T> | Get deserializer |
| configure | void | Configure |
| close | void | Close |

---

### 4.6 Header Interfaces

#### Header (Interface)

**Description**: Single header in a record.

**Stability**: @Stable

| Method | Return Type | Description |
|--------|-------------|-------------|
| key | String | Get header key |
| value | byte[] | Get header value |

---

#### Headers (Interface)

**Description**: Collection of headers.

**Stability**: @Stable

| Method | Return Type | Description |
|--------|-------------|-------------|
| add | Headers | Add header |
| add | Headers | Add header with bytes |
| remove | Headers | Remove header |
| headers | Iterable<Header> | Get headers by key |
| toArray | Header[] | Get all headers |
| iterator | Iterator<Header> | Iterate headers |

---

### 4.7 Config Classes

#### ConfigDef (Class)

**Description**: Definition of configuration parameters.

**Stability**: @Stable

#### Config (Class)

**Description**: Configuration values with validation.

**Stability**: @Stable

#### ConfigValue (Class)

**Description**: Single configuration value.

**Stability**: @Stable

---

### 4.8 Errors Classes

#### KafkaException (Class)

**Description**: Base exception for Kafka.

| Exception Class | Description |
|-----------------|-------------|
| TimeoutException | Operation timed out |
| InterruptException | Operation interrupted |
| WakeupException | Consumer wakeup called |
| AuthorizationException | Authorization failed |
| AuthenticationException | Authentication failed |
| UnsupportedVersionException | Version not supported |
| IllegalStateException | Invalid state |
| CommitFailedException | Commit failed |
| RecordTooLargeException | Record too large |
| SerializationException | Serialization error |
| DeserializationException | Deserialization error |
| BufferExhaustedException | Buffer exhausted |
| OutOfOrderSequenceException | Out of order sequence |
| ProducerFencedException | Producer fenced |

---

## 5. Streams API (org.apache.kafka.streams)

### 5.1 KafkaStreams

**Description**: Kafka Streams client for continuous computation.

**Stability**: @Stable

#### Constructors (7 methods)

| Method | Signature | Description |
|--------|-----------|-------------|
| KafkaStreams | `KafkaStreams(Topology topology, Properties props)` | Basic constructor |
| KafkaStreams | `KafkaStreams(Topology topology, Properties props, Time time)` | Constructor with time |
| KafkaStreams | `KafkaStreams(Topology topology, Properties props, KafkaClientSupplier clientSupplier)` | Constructor with client supplier |
| KafkaStreams | `KafkaStreams(Topology topology, Properties props, KafkaClientSupplier clientSupplier, Time time)` | Full constructor |
| KafkaStreams | `KafkaStreams(Topology topology, StreamsConfig applicationConfigs)` | Constructor with StreamsConfig |
| KafkaStreams | `KafkaStreams(Topology topology, StreamsConfig applicationConfigs, Time time)` | Constructor with StreamsConfig and time |
| KafkaStreams | `KafkaStreams(Topology topology, StreamsConfig applicationConfigs, KafkaClientSupplier clientSupplier)` | Constructor with StreamsConfig and supplier |

#### Public Methods (28 methods)

| Method | Return Type | Description | Stability |
|--------|-------------|-------------|-----------|
| start | void | Start the streams instance | @Stable |
| close | void | Close the instance | @Stable |
| close | boolean | Close with timeout | @Stable |
| close | boolean | Close with CloseOptions | @Stable |
| close | boolean | Close with deprecated CloseOptions | @Deprecated |
| cleanUp | void | Cleanup local state store | @Stable |
| state | State | Get current state | @Stable |
| setStateListener | void | Set state listener | @Stable |
| setUncaughtExceptionHandler | void | Set exception handler | @Stable |
| setGlobalStateRestoreListener | void | Set restore listener | @Stable |
| setStandbyUpdateListener | void | Set standby update listener | @Evolving |
| metrics | Map<MetricName,Metric> | Get metrics | @Stable |
| addStreamThread | Optional<String> | Add stream thread | @Evolving |
| removeStreamThread | Optional<String> | Remove stream thread | @Evolving |
| removeStreamThread | Optional<String> | Remove thread with timeout | @Evolving |
| metadataForAllStreamsClients | Collection<StreamsMetadata> | Get all streams metadata | @Stable |
| streamsMetadataForStore | Collection<StreamsMetadata> | Get metadata for store | @Stable |
| queryMetadataForKey | KeyQueryMetadata | Query metadata for key | @Stable |
| queryMetadataForKey | KeyQueryMetadata | Query metadata for key with partitioner | @Stable |
| store | T | Get state store | @Stable |
| pause | void | Pause processing | @Evolving |
| isPaused | boolean | Check if paused | @Evolving |
| resume | void | Resume processing | @Evolving |
| clientInstanceIds | ClientInstanceIds | Get client instance IDs | @Evolving |
| metadataForLocalThreads | Set<ThreadMetadata> | Get local thread metadata | @Stable |
| allLocalStorePartitionLags | Map | Get all store partition lags | @Evolving |
| query | StateQueryResult<R> | Interactive query | @Evolving |

#### Nested Classes (3 classes)

| Class | Description |
|-------|-------------|
| State | Enumeration of possible states |
| StateListener | Listener for state changes |
| CloseOptions | Options for closing (deprecated) |

---

### 5.2 StreamsBuilder

**Description**: Builder for Kafka Streams topology.

**Stability**: @Stable

#### Public Methods (15+ methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| stream | KStream<K,V> | Create stream from topic |
| stream | KStream<K,V> | Create stream from topics |
| stream | KStream<K,V> | Create stream with Consumed |
| table | KTable<K,V> | Create table from topic |
| table | KTable<K,V> | Create table with Materialized |
| globalTable | GlobalKTable<K,V> | Create global table |
| addSource | void | Add source node |
| addSink | void | Add sink node |
| addProcessor | void | Add processor node |
| addStateStore | void | Add state store |
| addGlobalStore | void | Add global store |
| build | Topology | Build topology |
| build | Topology | Build topology with props |

---

### 5.3 Topology

**Description**: Represents a stream processing topology.

**Stability**: @Stable

#### Public Methods (12+ methods)

| Method | Return Type | Description |
|--------|-------------|-------------|
| addSource | Topology | Add source |
| addSink | Topology | Add sink |
| addProcessor | Topology | Add processor |
| addStateStore | Topology | Add state store |
| addGlobalStore | Topology | Add global store |
| describe | TopologyDescription | Describe topology |
| subtopologies | Set | Get subtopologies |
| globalTopics | Set | Get global topics |
| describe | String | String description |

---

### 5.4 KStream<K,V> (Interface)

**Description**: Stream of key-value records.

**Stability**: @Stable

#### Public Methods (100+ methods - categorized)

**Filter/Transformation Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| filter | KStream<K,V> | Filter records |
| filterNot | KStream<K,V> | Filter inverse |
| map | KStream<KR,VOut> | Map key and value |
| mapValues | KStream<K,VOut> | Map values only |
| flatMap | KStream<KR,VOut> | Flat map |
| flatMapValues | KStream<K,VOut> | Flat map values |
| selectKey | KStream<KR,V> | Select new key |
| peek | KStream<K,V> | Peek at records |
| foreach | void | Iterate records |

**Branching/Merging Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| split | BranchedKStream | Split stream |
| merge | KStream<K,V> | Merge streams |
| repartition | KStream<K,V> | Repartition stream |

**Output Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| to | void | Write to topic |
| toTable | KTable<K,V> | Convert to table |
| print | void | Print records |

**Grouping Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| groupByKey | KGroupedStream<K,V> | Group by key |
| groupBy | KGroupedStream<KR,V> | Group by new key |

**Join Operations (KStream-KStream)**

| Method | Return Type | Description |
|--------|-------------|-------------|
| join | KStream<K,VOut> | Inner windowed join |
| leftJoin | KStream<K,VOut> | Left windowed join |
| outerJoin | KStream<K,VOut> | Outer windowed join |

**Join Operations (KStream-KTable)**

| Method | Return Type | Description |
|--------|-------------|-------------|
| join | KStream<K,VOut> | Inner join with table |
| leftJoin | KStream<K,VOut> | Left join with table |

**Join Operations (KStream-GlobalKTable)**

| Method | Return Type | Description |
|--------|-------------|-------------|
| join | KStream<K,VOut> | Inner join with global table |
| leftJoin | KStream<K,VOut> | Left join with global table |

**Processor Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| process | void | Process records (PAPI) |
| processValues | void | Process values (PAPI) |

---

### 5.5 KTable<K,V> (Interface)

**Description**: Table of key-value records (changelog stream).

**Stability**: @Stable

#### Public Methods (30+ methods)

**Transformation Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| filter | KTable<K,V> | Filter records |
| filterNot | KTable<K,V> | Filter inverse |
| mapValues | KTable<K,VOut> | Map values |
| flatMapValues | KTable<K,VOut> | Flat map values |

**Output Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| toStream | KStream<K,V> | Convert to stream |
| toStream | KStream<KR,V> | Convert with new key |

**Grouping Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| groupBy | KGroupedTable<KR,V> | Group by new key |

**Join Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| join | KTable<K,VOut> | Inner join with table |
| leftJoin | KTable<K,VOut> | Left join with table |
| outerJoin | KTable<K,VOut> | Outer join with table |

**Other Operations**

| Method | Return Type | Description |
|--------|-------------|-------------|
| suppress | KTable<K,V> | Suppress updates |
| query | StateQueryResult | Interactive query |

---

### 5.6 GlobalKTable<K,V> (Interface)

**Description**: Global table replicated to all instances.

**Stability**: @Stable

#### Public Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| query | StateQueryResult | Interactive query |

---

### 5.7 State Stores

#### StateStore (Interface)

**Description**: Interface for state stores.

| Method | Return Type | Description |
|--------|-------------|-------------|
| name | String | Get store name |
| init | void | Initialize store |
| flush | void | Flush to storage |
| close | void | Close store |
| persistent | boolean | Check persistence |

#### KeyValueStore<K,V> (Interface)

**Description**: Key-value state store.

| Method | Return Type | Description |
|--------|-------------|-------------|
| get | V | Get value by key |
| put | void | Put key-value |
| putAll | void | Put all entries |
| delete | V | Delete by key |
| range | KeyValueIterator | Get range |
| all | KeyValueIterator | Get all |

#### WindowStore<K,V> (Interface)

**Description**: Windowed state store.

| Method | Return Type | Description |
|--------|-------------|-------------|
| put | void | Put with timestamp |
| fetch | WindowStoreIterator | Fetch windowed records |
| fetch | KeyValueIterator | Fetch all windows |
| fetchAll | KeyValueIterator | Fetch all |

#### SessionStore<K,V> (Interface)

**Description**: Session state store.

| Method | Return Type | Description |
|--------|-------------|-------------|
| fetch | KeyValueIterator | Fetch session |

---

### 5.8 Time/Window Classes

#### TimeWindows (Class)

**Description**: Fixed-size time-based windows.

| Method | Return Type | Description |
|--------|-------------|-------------|
| of | TimeWindows | Create time windows |
| advanceBy | TimeWindows | Set advance interval |
| grace | TimeWindows | Set grace period |

#### SessionWindows (Class)

**Description**: Session-based windows.

| Method | Return Type | Description |
|--------|-------------|-------------|
| withInactivityGap | SessionWindows | Set gap |
| grace | SessionWindows | Set grace |

#### JoinWindows (Class)

**Description**: Windows for stream joins.

| Method | Return Type | Description |
|--------|-------------|-------------|
| of | JoinWindows | Create join window |
| before | JoinWindows | Set before time |
| after | JoinWindows | Set after time |
| grace | JoinWindows | Set grace |

---

## 6. Connect API (org.apache.kafka.connect)

### 6.1 Connector (Class)

**Description**: Base class for Connect connectors.

**Stability**: @Stable

#### Public Methods (9 methods)

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| initialize | void | `initialize(ConnectorContext ctx)` | Initialize connector |
| initialize | void | `initialize(ConnectorContext ctx, List<Map<String,String>> taskConfigs)` | Initialize with configs |
| start | void | `start(Map<String,String> props)` | Start connector (abstract) |
| reconfigure | void | `reconfigure(Map<String,String> props)` | Reconfigure |
| taskClass | Class<? extends Task> | `taskClass()` | Get task class (abstract) |
| taskConfigs | List<Map<String,String>> | `taskConfigs(int maxTasks)` | Get task configs (abstract) |
| stop | void | `stop()` | Stop connector (abstract) |
| validate | Config | `validate(Map<String,String> connectorConfigs)` | Validate config |
| config | ConfigDef | `config()` | Get config definition (abstract) |
| version | String | `version()` | Get version (from Versioned) |

---

### 6.2 SourceConnector (Class)

**Description**: Connector for source systems.

**Stability**: @Stable

**Inherits**: Connector

---

### 6.3 SinkConnector (Class)

**Description**: Connector for sink systems.

**Stability**: @Stable

**Inherits**: Connector

---

### 6.4 Task (Interface)

**Description**: Task interface for Connect work.

**Stability**: @Stable

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| start | void | `start(Map<String,String> props)` | Start task |
| stop | void | `stop()` | Stop task |
| version | String | `version()` | Get version |

---

### 6.5 SourceTask (Class)

**Description**: Task for source connectors.

**Stability**: @Stable

**Inherits**: Task

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| poll | List<SourceRecord> | `poll()` | Poll for records |
| commit | void | `commit()` | Commit offsets |
| commitRecord | void | `commitRecord(SourceRecord record)` | Commit single record |

---

### 6.6 SinkTask (Class)

**Description**: Task for sink connectors.

**Stability**: @Stable

**Inherits**: Task

| Method | Return Type | Signature | Description |
|--------|-------------|-----------|-------------|
| put | void | `put(Collection<SinkRecord> records)` | Process records |
| flush | Map<TopicPartition,OffsetAndMetadata> | `flush(Map<TopicPartition,OffsetAndMetadata> offsets)` | Flush records |
| preCommit | Map<TopicPartition,OffsetAndMetadata> | `preCommit(Map<TopicPartition,OffsetAndMetadata> offsets)` | Pre-commit |
| open | void | `open(Collection<TopicPartition> partitions)` | Open partitions |
| close | void | `close(Collection<TopicPartition> partitions)` | Close partitions |

---

### 6.7 SourceRecord

**Description**: Record from source connector.

**Stability**: @Stable

#### Constructors

| Method | Signature | Description |
|--------|-----------|-------------|
| SourceRecord | `SourceRecord(Map<String,String> sourcePartition, Map<String,String> sourceOffset, String topic, Integer partition, Schema keySchema, Object key, Schema valueSchema, Object value)` | Full constructor |
| SourceRecord | `SourceRecord(...)` | Various constructors |

#### Public Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| sourcePartition | Map<String,String> | Get source partition |
| sourceOffset | Map<String,String> | Get source offset |
| topic | String | Get topic |
| topicPartition | TopicPartition | Get topic partition |
| kafkaPartition | Integer | Get Kafka partition |
| keySchema | Schema | Get key schema |
| key | Object | Get key |
| valueSchema | Schema | Get value schema |
| value | Object | Get value |
| timestamp | Long | Get timestamp |
| headers | ConnectHeaders | Get headers |

---

### 6.8 SinkRecord

**Description**: Record for sink connector.

**Stability**: @Stable

**Inherits**: SourceRecord

#### Public Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| originalRecord | ConsumerRecord<?,?> | Get original consumer record |
| originalOffset | long | Get original offset |
| originalTopic | String | Get original topic |
| originalPartition | int | Get original partition |
| timestampType | TimestampType | Get timestamp type |

---

### 6.9 ConnectorContext (Interface)

**Description**: Context for connector to communicate with runtime.

**Stability**: @Stable

| Method | Return Type | Description |
|--------|-------------|-------------|
| requestTaskReconfiguration | void | Request reconfiguration |
| raiseError | void | Raise error |

---

### 6.10 Data/Schema Classes

#### Schema (Interface)

**Description**: Schema definition for Connect data.

| Method | Return Type | Description |
|--------|-------------|-------------|
| type | Schema.Type | Get schema type |
| name | String | Get name |
| version | Integer | Get version |
| doc | String | Get documentation |
| parameters | Map<String,String> | Get parameters |
| isOptional | boolean | Check optional |
| defaultValue | Object | Get default |
| fields | List<Field> | Get fields |
| field | Field | Get field by name |
| keySchema | Schema | Get key schema |
| valueSchema | Schema | Get value schema |

#### SchemaBuilder (Class)

**Description**: Builder for schemas.

| Method | Return Type | Description |
|--------|-------------|-------------|
| type | SchemaBuilder | Set type |
| name | SchemaBuilder | Set name |
| version | SchemaBuilder | Set version |
| doc | SchemaBuilder | Set doc |
| parameter | SchemaBuilder | Add parameter |
| optional | SchemaBuilder | Make optional |
| required | SchemaBuilder | Make required |
| defaultValue | SchemaBuilder | Set default |
| field | SchemaBuilder | Add field |
| build | Schema | Build schema |

#### Struct (Class)

**Description**: Structured data container.

| Method | Return Type | Description |
|--------|-------------|-------------|
| schema | Schema | Get schema |
| put | Struct | Put field value |
| get | Object | Get field value |
| validate | void | Validate struct |

---

## 7. Method Count Statistics

### Summary by Module

| Module | Main Classes | Methods Count |
|--------|--------------|---------------|
| Producer API | 5 | ~40 |
| Consumer API | 8 | ~120 |
| Admin API | 3 main + 50+ Result/Option classes | ~150+ |
| Common API | 15+ | ~100 |
| Streams API | 15+ | ~350+ |
| Connect API | 12+ | ~100 |

### Detailed Method Counts

#### Producer API

| Class | Constructors | Methods | Total |
|-------|--------------|---------|-------|
| KafkaProducer | 4 | 15 | 19 |
| ProducerRecord | 6 | 9 | 15 |
| RecordMetadata | 0 | 8 | 8 |
| Callback | 0 | 1 | 1 |
| Producer (Interface) | 0 | 15 | 15 |

**Producer API Total: ~58 methods**

#### Consumer API

| Class | Constructors | Methods | Total |
|-------|--------------|---------|-------|
| KafkaConsumer | 4 | 52 | 56 |
| ConsumerRecord | 3 | 13 | 16 |
| ConsumerRecords | 0 | 8 | 8 |
| ConsumerRebalanceListener | 0 | 2 | 2 |
| OffsetAndMetadata | 3 | 3 | 6 |
| ConsumerGroupMetadata | 0 | 4 | 4 |
| OffsetCommitCallback | 0 | 1 | 1 |
| Consumer (Interface) | 0 | 52 | 52 |

**Consumer API Total: ~145 methods**

#### Admin API

| Class/Interface | Methods Count |
|-----------------|---------------|
| Admin (Interface) | 105+ |
| AdminClient | 105+ (same) |
| CreateTopicsResult | 2 |
| DeleteTopicsResult | 2 |
| ListTopicsResult | 2 |
| DescribeTopicsResult | 2 |
| DescribeClusterResult | 3 |
| DescribeConfigsResult | 2 |
| CreateAclsResult | 2 |
| DeleteAclsResult | 2 |
| DescribeAclsResult | 1 |
| AlterConfigsResult | 2 |
| AlterClientQuotasResult | 2 |
| DescribeClientQuotasResult | 1 |
| CreatePartitionsResult | 2 |
| DeleteRecordsResult | 2 |
| ListConsumerGroupsResult | 2 |
| DescribeConsumerGroupsResult | 2 |
| DeleteConsumerGroupsResult | 2 |
| ListConsumerGroupOffsetsResult | 2 |
| AlterConsumerGroupOffsetsResult | 2 |
| ... (50+ more Result classes) | ... |

**Admin API Total: ~250+ methods**

#### Common API

| Class | Methods Count |
|-------|---------------|
| TopicPartition | 5 |
| PartitionInfo | 6 |
| Node | 7 |
| Cluster | 11 |
| Serializer | 4 |
| Deserializer | 4 |
| Serde | 4 |
| Header | 2 |
| Headers | 6 |
| ConfigDef | 30+ |
| Config | 5+ |
| ConfigValue | 5+ |
| Errors (15+ exceptions) | ~30 |

**Common API Total: ~130+ methods**

#### Streams API

| Class/Interface | Methods Count |
|-----------------|---------------|
| KafkaStreams | 28 |
| StreamsBuilder | 15+ |
| Topology | 12+ |
| KStream | 100+ |
| KTable | 30+ |
| GlobalKTable | 5+ |
| StateStore | 5+ |
| KeyValueStore | 6+ |
| WindowStore | 5+ |
| SessionStore | 3+ |
| TimeWindows | 4+ |
| SessionWindows | 3+ |
| JoinWindows | 5+ |
| KGroupedStream | 15+ |
| KGroupedTable | 10+ |
| Materialized | 10+ |
| Consumed | 5+ |
| Produced | 5+ |
| Repartitioned | 5+ |
| StreamJoined | 5+ |
| ... | ... |

**Streams API Total: ~350+ methods**

#### Connect API

| Class | Methods Count |
|-------|---------------|
| Connector | 10 |
| SourceConnector | 2+ |
| SinkConnector | 2+ |
| Task | 3 |
| SourceTask | 4+ |
| SinkTask | 6+ |
| SourceRecord | 10+ |
| SinkRecord | 6+ |
| ConnectorContext | 2 |
| Schema | 12+ |
| SchemaBuilder | 15+ |
| Struct | 5+ |

**Connect API Total: ~100+ methods**

---

## Grand Total Statistics

| API Module | Method Count |
|------------|--------------|
| Producer API | ~58 |
| Consumer API | ~145 |
| Admin API | ~250+ |
| Common API | ~130+ |
| Streams API | ~350+ |
| Connect API | ~100+ |
| **TOTAL** | **~1030+ methods** |

---

## Stability Annotations Reference

| Annotation | Description |
|------------|-------------|
| @Stable | API is stable and unlikely to change |
| @Evolving | API may evolve in future releases |
| @Deprecated | API is deprecated, avoid use |
| @Unstable | API is experimental/unstable |
| @InterfaceStability | Marks stability at class level |

---

## Document Information

- **Version**: Kafka 4.2.0
- **Source**: https://kafka.apache.org/42/javadoc/
- **Generated Date**: 2025
- **Total Classes Covered**: 100+
- **Total Methods Documented**: 1030+

---

## References

1. Kafka Producer API: https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/producer/package-summary.html
2. Kafka Consumer API: https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/package-summary.html
3. Kafka Admin API: https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/admin/package-summary.html
4. Kafka Common API: https://kafka.apache.org/42/javadoc/org/apache/kafka/common/package-summary.html
5. Kafka Streams API: https://kafka.apache.org/42/javadoc/org/apache/kafka/streams/package-summary.html
6. Kafka Connect API: https://kafka.apache.org/42/javadoc/org/apache/kafka/connect/package-summary.html

---

*End of Kafka Java API Complete List*