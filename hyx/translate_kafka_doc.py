#!/usr/bin/env python3
"""
将Kafka Java API文档从英文翻译成中文
"""

# 翻译映射字典
TRANSLATIONS = {
    # 表头翻译
    "Description": "说明",
    "Method": "方法名",
    "Return Type": "返回类型",
    "Signature": "参数",
    "Stability": "稳定性",
    "Field": "字段",
    "Type": "类型",
    "Value": "值",
    "Constructors": "构造方法",
    "Public Methods": "公共方法",
    "Constants": "常量",
    "Static Methods": "静态方法",
    "Instance Methods": "实例方法",
    
    # 稳定性标注
    "@Stable": "稳定",
    "@Evolving": "演进中",
    "@Deprecated": "已弃用",
    
    # 常用描述翻译
    "A Kafka client that publishes records to the Kafka cluster. Thread-safe.": "Kafka生产者，发送消息到Kafka集群。线程安全。",
    "A key/value pair to be sent to Kafka.": "发送到Kafka的消息记录（键值对）。",
    "Metadata for a record that has been acknowledged by the server.": "消息元数据，包含发送成功后的信息。",
    "Callback interface for asynchronous operations.": "回调接口，用于异步操作的回调。",
    "Producer interface defining common operations.": "生产者接口，定义通用操作。",
    
    "A client that consumes records from a Kafka cluster. NOT thread-safe.": "Kafka消费者，从Kafka集群消费消息。非线程安全。",
    "A key/value pair received from Kafka.": "从Kafka收到的消息记录。",
    "A container for ConsumerRecord objects.": "消费者记录集合，poll返回的结果。",
    "Listener for consumer rebalance events.": "Rebalance监听器，监听分区重新分配事件。",
    "Offset and metadata for committing.": "偏移量和元数据，用于提交消费进度。",
    
    "Get topic name": "获取Topic名称",
    "Get partition": "获取分区号",
    "Get offset": "获取偏移量",
    "Get timestamp": "获取时间戳",
    "Get key": "获取Key",
    "Get value": "获取Value",
    "Get headers": "获取消息头",
    "Get serialized key size": "获取Key序列化大小",
    "Get serialized value size": "获取Value序列化大小",
    
    "Instantiate with Map configuration": "使用Map配置创建",
    "Instantiate with Properties": "使用Properties创建",
    "Instantiate with Map and custom serializers": "使用Map和自定义序列化器创建",
    "Instantiate with Properties and custom serializers": "使用Properties和自定义序列化器创建",
    
    "Initialize transactions. Must be called first when transactional.id is set": "初始化事务。启用事务前必须调用",
    "Start a new transaction": "开始事务",
    "Commit the ongoing transaction": "提交事务",
    "Abort the ongoing transaction": "中止事务",
    "Asynchronously send a record to a topic": "异步发送消息到Topic",
    "Asynchronously send with callback": "异步发送消息（带回调）",
    "Make all buffered records available to send and block on completion": "刷新缓冲区，等待所有消息发送完成",
    "Get partition metadata for a topic": "获取Topic分区元数据",
    "Get full set of internal metrics": "获取完整指标集",
    "Close the producer": "关闭生产者",
    "Close with timeout": "关闭生产者（带超时）",
    
    "Subscribe to topics": "订阅Topic",
    "Subscribe with rebalance listener": "订阅Topic（带Rebalance监听器）",
    "Subscribe using regex pattern": "使用正则表达式订阅Topic",
    "Unsubscribe from all topics": "取消订阅",
    "Manually assign partitions": "手动分配分区",
    "Poll for new records": "消费消息",
    "Commit offsets synchronously": "同步提交偏移量",
    "Commit with timeout": "同步提交（带超时）",
    "Commit specific offsets": "提交指定偏移量",
    "Commit asynchronously": "异步提交偏移量",
    "Commit async with callback": "异步提交（带回调）",
    "Seek to specific offset": "跳转到指定偏移量",
    "Seek to beginning": "跳到起始位置",
    "Seek to end": "跳到末尾位置",
    "Get current position": "获取当前偏移量",
    "Get committed offsets": "获取已提交的偏移量",
    "Pause partitions": "暂停分区",
    "Resume partitions": "恢复分区",
    "Get paused partitions": "获取暂停的分区",
    "Get partition info": "获取分区信息",
    "List all topics": "列出所有Topic",
    "Close consumer": "关闭消费者",
    "Wakeup consumer from blocking operation": "唤醒消费者（中断阻塞操作）",
    "Force rebalance": "强制Rebalance",
    
    "Create topics": "创建Topic",
    "Delete topics": "删除Topic",
    "List topics": "列出Topic",
    "Describe topics": "描述Topic详情",
    "Describe cluster": "描述集群信息",
    "Create partitions": "增加分区",
    "Describe configs": "获取配置",
    "Alter configs": "修改配置",
    "Create ACLs": "创建ACL",
    "Delete ACLs": "删除ACL",
    "List ACLs": "列出ACL",
    
    "Get topic": "获取Topic",
    "Get partition number": "获取分区号",
    "Get offset position": "获取偏移量位置",
    "Get timestamp in milliseconds": "获取时间戳（毫秒）",
    "Get timestamp type": "获取时间戳类型",
    
    "Total number of records": "记录总数",
    "Check if empty": "是否为空",
    "Get partitions with data": "获取有数据的分区",
    "Iterator over all records": "迭代所有记录",
    
    "Called when partitions revoked": "分区被撤销时调用",
    "Called when partitions assigned": "分区被分配时调用",
}

# 方法名翻译（部分关键方法）
METHOD_TRANSLATIONS = {
    "send": "发送消息",
    "flush": "刷新缓冲区",
    "close": "关闭",
    "poll": "消费消息",
    "subscribe": "订阅Topic",
    "unsubscribe": "取消订阅",
    "assign": "手动分配分区",
    "commitSync": "同步提交",
    "commitAsync": "异步提交",
    "seek": "跳转偏移量",
    "seekToBeginning": "跳到起始",
    "seekToEnd": "跳到末尾",
    "position": "获取当前偏移量",
    "committed": "获取已提交偏移量",
    "pause": "暂停分区",
    "resume": "恢复分区",
    "beginTransaction": "开始事务",
    "commitTransaction": "提交事务",
    "abortTransaction": "中止事务",
    "initTransactions": "初始化事务",
    
    "createTopics": "创建Topic",
    "deleteTopics": "删除Topic",
    "listTopics": "列出Topic",
    "describeTopics": "描述Topic",
    "describeCluster": "描述集群",
    "createPartitions": "增加分区",
    "describeConfigs": "获取配置",
    "alterConfigs": "修改配置",
    
    "topic": "获取Topic",
    "partition": "获取分区",
    "offset": "获取偏移量",
    "timestamp": "获取时间戳",
    "key": "获取Key",
    "value": "获取Value",
    "headers": "获取消息头",
    
    "count": "获取记录数",
    "isEmpty": "是否为空",
    "iterator": "获取迭代器",
    "partitions": "获取分区列表",
    
    "filter": "过滤记录",
    "map": "映射记录",
    "mapValues": "映射Value",
    "flatMap": "扁平映射",
    "flatMapValues": "扁平映射Value",
    "groupBy": "分组",
    "groupByKey": "按Key分组",
    "reduce": "聚合",
    "count": "计数",
    "aggregate": "自定义聚合",
    "join": "连接",
    "leftJoin": "左连接",
    "merge": "合并",
    "to": "输出到Topic",
    
    "start": "启动",
    "stop": "停止",
    "state": "获取状态",
    "metrics": "获取指标",
    "cleanUp": "清理状态",
}

def translate_doc(input_file, output_file):
    """翻译文档"""
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 替换表头
    content = content.replace("| Method | Signature | Description |", "| 方法名 | 参数 | 说明 |")
    content = content.replace("| Method | Return Type | Signature | Description | Stability |", 
                              "| 方法名 | 返回类型 | 参数 | 说明 | 稳定性 |")
    content = content.replace("| Method | Return Type | Description |", "| 方法名 | 返回类型 | 说明 |")
    content = content.replace("| Field | Type | Value | Description |", "| 字段 | 类型 | 值 | 说明 |")
    
    # 替换稳定性标注
    content = content.replace("@Stable", "稳定")
    content = content.replace("@Evolving", "演进中")
    content = content.replace("@Deprecated", "已弃用")
    
    # 替换小节标题
    content = content.replace("Constructors", "构造方法")
    content = content.replace("Public Methods", "公共方法")
    content = content.replace("Constants", "常量")
    content = content.replace("Static Methods", "静态方法")
    content = content.replace("Instance Methods", "实例方法")
    content = content.replace("Fields", "字段")
    
    # 替换Description
    content = content.replace("**Description**:", "**说明**:")
    
    # 替换常用描述
    for eng, cn in TRANSLATIONS.items():
        content = content.replace(eng, cn)
    
    # 保存
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return content

if __name__ == '__main__':
    input_file = '/home/h00517772/spark/hyx/kafka_java_api_complete_list.md'
    output_file = '/home/h00517772/spark/hyx/kafka_java_api_中文版.md'
    
    print("翻译Kafka API文档...")
    translate_doc(input_file, output_file)
    
    # 统计
    with open(output_file, 'r') as f:
        content = f.read()
    
    classes = content.count('### ')
    methods = len([line for line in content.split('\n') if line.startswith('| ') and '方法名' not in line and '--------' not in line])
    
    print(f"  类数量: {classes}")
    print(f"  方法数量: {methods}")
    print(f"  输出: {output_file}")
    print("\n完成")
