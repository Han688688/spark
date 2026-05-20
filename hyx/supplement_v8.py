#!/usr/bin/env python3
"""
补充遗漏的Streaming类和方法 - 第八轮
"""
import re

STREAMING_CLASSES = '''
### JavaInputDStream[T]
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: Java版本的InputDStream，是JavaReceiverInputDStream的父类。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `start` | 无 | `Unit` | 启动接收器 | `inputDStream.start();` |
| `stop` | 无 | `Unit` | 停止接收器 | `inputDStream.stop();` |
| `compute` | Time validTime | `Option[RDD[T]]` | 计算指定时间的RDD | `Option<JavaRDD<String>> rdd = inputDStream.compute(time);` |
| `isInitialized` | 无 | `boolean` | 是否已初始化 | `boolean init = inputDStream.isInitialized();` |
| `slideDuration` | 无 | `Duration` | 获取滑动间隔 | `Duration duration = inputDStream.slideDuration();` |

---

### JavaReceiverInputDStream[T]
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: Java版本的ReceiverInputDStream，用于自定义数据接收器。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `start` | 无 | `Unit` | 启动接收器 | `receiverInputDStream.start();` |
| `stop` | 无 | `Unit` | 停止接收器 | `receiverInputDStream.stop();` |
| `receiver` | 无 | `Receiver[T]` | 获取底层Receiver | `Receiver<String> receiver = receiverInputDStream.receiver();` |
| `compute` | Time validTime | `Option[RDD[T]]` | 计算指定时间的RDD | `Option<JavaRDD<String>> rdd = receiverInputDStream.compute(time);` |
| `isInitialized` | 无 | `boolean` | 是否已初始化 | `boolean init = receiverInputDStream.isInitialized();` |
| `slideDuration` | 无 | `Duration` | 获取滑动间隔 | `Duration duration = receiverInputDStream.slideDuration();` |
| `storageLevel` | 无 | `StorageLevel` | 获取存储级别 | `StorageLevel level = receiverInputDStream.storageLevel();` |
| `repartition` | int numPartitions | `JavaDStream[T]` | 重新分区 | `JavaDStream<String> repartitioned = receiverInputDStream.repartition(4);` |

---

'''

STREAMING_METHODS = '''
| `removeStreamingListener` | StreamingListener listener | `Unit` | 移除流处理监听器 | `jssc.removeStreamingListener(listener);` |
| `getActiveContexts` | 无 | `List[StreamingContext]` | 获取所有活动的StreamingContext | `List<StreamingContext> contexts = StreamingContext.getActiveContexts();` |
'''

def add_supplements(filepath):
    """补充遗漏的Streaming类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 补充JavaStreamingContext方法
    if "### JavaStreamingContext" in content:
        jsc_start = content.find("### JavaStreamingContext")
        next_class = content.find("\n### ", jsc_start + 1)
        if next_class != -1:
            class_section = content[jsc_start:next_class]
            last_match = None
            for match in re.finditer(r'\| `[^`]+` \|.*?\n', class_section):
                last_match = match
            if last_match:
                insert_pos = jsc_start + last_match.end()
                content = content[:insert_pos] + STREAMING_METHODS + "\n" + content[insert_pos:]
                added_count += STREAMING_METHODS.count('| `')
                print(f"✅ JavaStreamingContext: 补充 2 个方法")
    
    # 添加Streaming类 - 在JavaDStream之后插入
    if "### JavaDStream[T]" in content:
        dstream_pos = content.find("### JavaDStream[T]")
        next_class = content.find("\n### ", dstream_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + STREAMING_CLASSES + content[insert_pos:]
            added_count += STREAMING_CLASSES.count('| `')
            print(f"✅ 添加JavaInputDStream和JavaReceiverInputDStream类")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的Streaming类和方法（第八轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
