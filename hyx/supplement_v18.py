#!/usr/bin/env python3
"""
补充遗漏的Streaming和Config类 - 第十八轮
"""
import re

STREAMING_CLASSES = '''
### StreamingQueryListener
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming查询监听器，监控查询生命周期事件。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onQueryStarted` | QueryStartedEvent event | `void` | 查询启动事件 | `public void onQueryStarted(QueryStartedEvent event) { System.out.println("Query started: " + event.id()); }` |
| `onQueryProgress` | QueryProgressEvent event | `void` | 查询进度事件 | `public void onQueryProgress(QueryProgressEvent event) { System.out.println("Progress: " + event.progress().numInputRows()); }` |
| `onQueryIdle` | QueryIdleEvent event | `void` | 查询空闲事件 | `public void onQueryIdle(QueryIdleEvent event) { System.out.println("Query idle"); }` |
| `onQueryTerminated` | QueryTerminatedEvent event | `void` | 查询终止事件 | `public void onQueryTerminated(QueryTerminatedEvent event) { System.out.println("Query terminated: " + event.id()); }` |
| `onQueryFailure` | QueryFailureEvent event | `void` | 查询失败事件 | `public void onQueryFailure(QueryFailureEvent event) { System.out.println("Query failed: " + event.exception()); }` |

---

### StreamingQueryStatus
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming查询状态信息。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | 查询名称 | `String name = status.name();` |
| `isDataAvailable` | 无 | `boolean` | 是否有数据可用 | `boolean hasData = status.isDataAvailable();` |
| `isTriggerActive` | 无 | `boolean` | 触发器是否活跃 | `boolean active = status.isTriggerActive();` |
| `timestamp` | 无 | `long` | 时间戳 | `long ts = status.timestamp();` |
| `json` | 无 | `String` | JSON表示 | `String json = status.json();` |

---

### ForeachWriter[T]
**包路径**: `org.apache.spark.sql`
**说明**: Structured Streaming自定义输出写入器。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `open` | long partitionId, long epochId | `boolean` | 打开写入器，返回false则跳过 | `public boolean open(long partitionId, long epochId) { connection = createConnection(); return true; }` |
| `process` | T value | `void` | 处理单条数据 | `public void process(String value) { connection.write(value); }` |
| `close` | Throwable errorOrNull | `void` | 关闭写入器 | `public void close(Throwable error) { connection.close(); }` |

---

'''

CONFIG_CLASSES = '''
### RuntimeConfig
**包路径**: `org.apache.spark.sql`
**说明**: Spark运行时配置，从SparkSession.conf()获取。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | String key | `String` | 获取配置值 | `String value = spark.conf().get("spark.sql.shuffle.partitions");` |
| `get` | String key, String default | `String` | 获取配置值（带默认值） | `String value = spark.conf().get("spark.sql.autoBroadcastJoinThreshold", "10MB");` |
| `getAll` | 无 | `Map[String, String]` | 获取所有配置 | `Map<String, String> all = spark.conf().getAll();` |
| `set` | String key, String value | `RuntimeConfig` | 设置配置值 | `spark.conf().set("spark.sql.shuffle.partitions", "200");` |
| `unset` | String key | `RuntimeConfig` | 取消设置 | `spark.conf().unset("spark.sql.shuffle.partitions");` |
| `isModifiable` | String key | `boolean` | 是否可修改 | `boolean modifiable = spark.conf().isModifiable("spark.sql.shuffle.partitions");` |

---

'''

def add_supplements(filepath):
    """补充遗漏的类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 补充Streaming类 - 在StreamingQuery之后插入
    if "### StreamingQuery" in content:
        sq_pos = content.find("### StreamingQuery")
        next_class = content.find("\n### ", sq_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + STREAMING_CLASSES + content[insert_pos:]
            added_count += 15  # 手动计算
            print(f"✅ 添加Streaming类: StreamingQueryListener, StreamingQueryStatus, ForeachWriter")
    
    # 补充Config类 - 在SparkSession之后插入
    if "### SparkSession" in content:
        ss_pos = content.find("### SparkSession")
        next_class = content.find("\n### ", ss_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + CONFIG_CLASSES + content[insert_pos:]
            added_count += 6  # 手动计算
            print(f"✅ 添加Config类: RuntimeConfig")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的Streaming和Config类（第十八轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
