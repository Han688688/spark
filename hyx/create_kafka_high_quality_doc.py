#!/usr/bin/env python3
"""
从现有Kafka API文档生成高质量完整文档
"""

import re

# 快速入门示例
QUICK_START = '''# Kafka Java API 高质量完整文档

> **文档特点**:
> - 包含Kafka所有public Java API（1484个方法）
> - 核心方法提供完整可运行示例
> - 按业务分类组织，便于测试覆盖
> - 基于Kafka 4.2.0版本

> **说明**: 
> - 所有类和方法都是public，可直接调用测试
> - 标注@Stable的API稳定可用
> - 标注@Evolving的API可能变化

---

## 快速入门

### 1. Producer示例 - 发送消息到Kafka

```java
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaProducerExample {
    public static void main(String[] args) {
        // 1. 配置Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        
        // 2. 创建Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // 3. 发送消息（异步带回调）
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>("test-topic", "key-" + i, "value-" + i);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("发送成功: %s-%d@%d\\n", 
                        metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }
        
        // 4. 刷新并关闭
        producer.flush();
        producer.close();
    }
}
```

### 2. Consumer示例 - 从Kafka消费消息

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;
import java.util.Arrays;
import java.time.Duration;

public class KafkaConsumerExample {
    public static void main(String[] args) {
        // 1. 配置Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        
        // 2. 创建Consumer并订阅Topic
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-topic"));
        
        // 3. 消费消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("收到消息: %s-%d@%d key=%s value=%s\\n",
                    record.topic(), record.partition(), record.offset(),
                    record.key(), record.value());
            }
        }
    }
}
```

### 3. Admin示例 - 创建Topic

```java
import org.apache.kafka.clients.admin.*;
import java.util.Properties;
import java.util.Collections;
import java.util.Set;

public class KafkaAdminExample {
    public static void main(String[] args) throws Exception {
        // 1. 配置AdminClient
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        
        // 2. 创建AdminClient
        AdminClient admin = AdminClient.create(props);
        
        // 3. 创建Topic（3分区，1副本）
        NewTopic newTopic = new NewTopic("test-topic", 3, (short) 1);
        admin.createTopics(Collections.singletonList(newTopic)).all().get();
        
        // 4. 列出所有Topic
        Set<String> topics = admin.listTopics().names().get();
        System.out.println("Topics: " + topics);
        
        // 5. 关闭AdminClient
        admin.close();
    }
}
```

### 4. Streams示例 - 流处理

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;
import java.util.Properties;

public class KafkaStreamsExample {
    public static void main(String[] args) {
        // 1. 配置Streams
        Properties props = new Properties();
        props.put("application.id", "stream-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());
        
        // 2. 创建StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();
        
        // 3. 构建流处理逻辑
        KStream<String, String> stream = builder.stream("input-topic");
        stream.filter((key, value) -> value.length() > 5)
              .mapValues(value -> value.toUpperCase())
              .to("output-topic");
        
        // 4. 创建并启动KafkaStreams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
        // 5. 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

---

'''

# 示例模板字典
EXAMPLE_TEMPLATES = {
    # Producer
    "KafkaProducer<K,V>": '''// KafkaProducer: 发送消息到Kafka
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
KafkaProducer<String, String> producer = new KafkaProducer<>(props);''',
    
    "ProducerRecord<K,V>": '''// ProducerRecord: 创建发送记录
ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");''',
    
    "RecordMetadata": '''// RecordMetadata: 发送成功后的元数据
String topic = metadata.topic();
int partition = metadata.partition();
long offset = metadata.offset();''',
    
    # Consumer
    "KafkaConsumer<K,V>": '''// KafkaConsumer: 从Kafka消费消息
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "test-group");
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("topic"));''',
    
    "ConsumerRecord<K,V>": '''// ConsumerRecord: 收到的消息
String topic = record.topic();
String key = record.key();
String value = record.value();''',
    
    "ConsumerRecords<K,V>": '''// ConsumerRecords: 消息集合
for (ConsumerRecord<String, String> record : records) {
    System.out.println(record.value());
}''',
    
    # Admin
    "AdminClient": '''// AdminClient: 执行管理操作
AdminClient admin = AdminClient.create(props);
Set<String> topics = admin.listTopics().names().get();''',
    
    "NewTopic": '''// NewTopic: 创建Topic定义
NewTopic topic = new NewTopic("test", 3, (short) 1);''',
    
    # Streams
    "KafkaStreams": '''// KafkaStreams: 流处理应用
KafkaStreams streams = new KafkaStreams(topology, props);
streams.start();''',
    
    "StreamsBuilder": '''// StreamsBuilder: 构建流处理拓扑
StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> stream = builder.stream("input-topic");''',
    
    "KStream<K,V>": '''// KStream: 处理记录流
KStream<String, String> filtered = stream.filter((k, v) -> v.length() > 5);''',
    
    "KTable<K,V>": '''// KTable: 处理表（聚合结果）
KTable<String, Long> counts = stream.groupByKey().count();''',
    
    # Serialization
    "StringSerializer": '''// StringSerializer: String序列化器
StringSerializer serializer = new StringSerializer();''',
    
    "StringDeserializer": '''// StringDeserializer: String反序列化器
StringDeserializer deserializer = new StringDeserializer();''',
    
    # Common
    "TopicPartition": '''// TopicPartition: Topic+分区标识
TopicPartition tp = new TopicPartition("topic", 0);''',
    
    "OffsetAndMetadata": '''// OffsetAndMetadata: 偏移量+元数据
OffsetAndMetadata offset = new OffsetAndMetadata(100, "metadata");''',
}

def get_example(class_name, method_name):
    """获取方法示例"""
    # 构造方法
    if method_name == class_name.split('<')[0]:
        return EXAMPLE_TEMPLATES.get(class_name, "// 创建实例")
    
    # 根据方法名生成示例
    if method_name == "send":
        return "producer.send(new ProducerRecord<>(\"topic\", \"key\", \"value\"));"
    elif method_name == "poll":
        return "ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));"
    elif method_name == "subscribe":
        return "consumer.subscribe(Arrays.asList(\"topic1\", \"topic2\"));"
    elif method_name == "commitSync":
        return "consumer.commitSync();"
    elif method_name == "close":
        return "producer.close();"
    elif method_name == "flush":
        return "producer.flush();"
    elif method_name == "topic":
        return "String topic = record.topic();"
    elif method_name == "partition":
        return "int partition = record.partition();"
    elif method_name == "offset":
        return "long offset = record.offset();"
    elif method_name == "key":
        return "K key = record.key();"
    elif method_name == "value":
        return "V value = record.value();"
    elif method_name == "count":
        return "int count = records.count();"
    elif method_name == "createTopics":
        return "admin.createTopics(Collections.singletonList(newTopic)).all().get();"
    elif method_name == "listTopics":
        return "Set<String> topics = admin.listTopics().names().get();"
    elif method_name == "start":
        return "streams.start();"
    elif method_name == "filter":
        return "KStream<String, String> filtered = stream.filter((k, v) -> v.length() > 5);"
    elif method_name == "mapValues":
        return "KStream<String, String> mapped = stream.mapValues(v -> v.toUpperCase());"
    
    return "// 调用方法"

def convert_doc(input_file, output_file):
    """转换文档格式"""
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 添加快速入门
    output = QUICK_START
    
    # 解析原始文档
    sections = re.split(r'## \d+\.', content)
    
    # 转换每个部分
    for section in sections[1:]:  # 跳过第一个空部分
        if section.strip():
            # 提取部分标题
            title_match = re.search(r'\s+(.*?)\s+\(org\.apache\.kafka', section)
            if title_match:
                section_title = title_match.group(1).strip()
                output += f"## {section_title}\n\n"
            
            # 处理每个类
            classes = re.split(r'### \d+\.\d+', section)
            for cls in classes[1:]:
                if cls.strip():
                    # 提取类名
                    class_match = re.search(r'\s+(.*?)\s*\n', cls)
                    if class_match:
                        class_name = class_match.group(1).strip()
                        output += f"### {class_name}\n"
                        
                        # 提取包路径
                        pkg_match = re.search(r'\(org\.apache\.kafka\.(.*?)\)', section)
                        if pkg_match:
                            output += f"**包路径**: `org.apache.kafka.{pkg_match.group(1)}`\n"
                        
                        # 提取描述
                        desc_match = re.search(r'\*Description\*:\s*(.*?)\n', cls)
                        if desc_match:
                            output += f"**说明**: {desc_match.group(1).strip()}\n"
                        
                        # 提取方法数量
                        methods_match = re.search(r'Public Methods \((\d+)', cls)
                        if methods_match:
                            output += f"**方法数量**: {methods_match.group(1)}\n\n"
                            output += "| 方法名 | 参数 | 返回类型 | 描述 | 示例 |\n"
                            output += "|--------|------|----------|------|------|\n"
                            
                            # 提取方法行
                            method_lines = re.findall(r'\| (\w+) \|.*?\| (.*?) \|', cls)
                            for method_line in method_lines:
                                method_name = method_line[0]
                                return_type = method_line[1]
                                example = get_example(class_name, method_name)
                                output += f"| `{method_name}` | - | `{return_type}` | - | `{example}` |\n"
                        
                        output += "\n---\n\n"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(output)
    
    return output

if __name__ == '__main__':
    input_file = '/home/h00517772/spark/hyx/kafka_java_api_complete_list.md'
    output_file = '/home/h00517772/spark/hyx/kafka_java_api_高质量完整文档.md'
    
    print("转换Kafka API文档...")
    convert_doc(input_file, output_file)
    
    # 统计
    with open(output_file, 'r') as f:
        content = f.read()
    classes = content.count('### ')
    methods = content.count('| `')
    
    print(f"  类数量: {classes}")
    print(f"  方法数量: {methods}")
    print(f"  输出: {output_file}")
    print("\n完成")
