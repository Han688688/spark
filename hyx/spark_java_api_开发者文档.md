# Spark Java API 开发者文档

> **说明**: 本文档包含数据源开发者、插件开发者使用的public API。
> 普通用户通常不直接调用这些API。

---

## 文档结构

### AvroCompressionCodec
**包路径**: `org.apache.spark.sql.avro`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | s: String | `AvroCompressionCodec` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |
| `getCodecName` | 无 | `String` | 获取压缩编解码器的名称 | 返回压缩编解码器名称（如"snappy"、"deflate"） |
| `getSupportCompressionLevel` | 无 | `boolean` | 检查是否支持压缩级别配置 | 检查编解码器是否支持自定义压缩级别 |
| `lowerCaseName` | 无 | `String` | 转换为小写的名称 | 返回编解码器名称的小写形式 |

--------|------|----------|------|------|
| `bitmapAndMerge` | bitmap1: byte&lt;&gt;, bitmap2: byte&lt;&gt; | `void` | 对两个位图执行AND合并操作 | 对两个位图执行AND操作，返回交集位图 |
| `bitmapBitPosition` | value: long | `long` | 计算位图中指定值的位位置 | 计算值在桶内的位位置（0-63） |
| `bitmapBucketNumber` | value: long | `long` | 计算位图中指定值的桶编号 | 计算值所在的桶编号 |
| `bitmapCount` | bitmap: byte&lt;&gt; | `long` | 统计位图中设置的位数 | 返回位图中设置的位数统计 |
| `bitmapMerge` | bitmap1: byte&lt;&gt;, bitmap2: byte&lt;&gt; | `void` | 合并两个位图 | 合并两个位图，返回OR结果 |

--------|------|----------|------|------|
| `append` | row: InternalRow | `void` | 追加元素 | 向缓冲迭代器追加一行数据 |
| `durationMs` | 无 | `long` | 获取执行耗时（毫秒） | 返回执行耗时（毫秒） |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `incPeakExecutionMemory` | size: long | `void` | 增加峰值执行内存计数 | 增加峰值执行内存统计 |
| `next` | 无 | `InternalRow` | 获取迭代器的下一个元素 | 获取迭代器下一行数据 |
| `shouldStop` | 无 | `boolean` | 检查是否应该停止迭代 | 检查是否应停止迭代处理 |

--------|------|----------|------|------|
| `getSrid` | stringId: String | `Integer` | 获取空间参考系统ID（SRID） | 将字符串空间参考ID转换为整数SRID |
| `getStringId` | srid: int | `String` | 将SRID转换为字符串标识 | 将整数SRID转换为字符串标识 |


### CaseInsensitiveStringMap
**包路径**: `org.apache.spark.sql.util`
**说明**: 大小写不敏感的字符串键值映射，用于传递配置选项到数据源实现。所有key在内部转换为小写存储，确保key匹配时忽略大小写差异。
**方法数量**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `asCaseSensitiveMap` | 无 | `Map&lt;String, String&gt;` | 转换为保留原始大小写的不可变Map，用于需要区分大小写的场景 | `Map&lt;String, String&gt; original = map.asCaseSensitiveMap();
// 返回原始key大小写的Map` |
| `clear` | 无 | `void` | 清空集合（此实现不支持，抛出UnsupportedOperationException） | `// 注意：此方法会抛出异常
// CaseInsensitiveStringMap是不可变的` |
| `containsKey` | key: Object | `boolean` | 检查指定key是否存在（大小写不敏感匹配） | `boolean exists = map.containsKey("Path");
// 即使内部存储为"path"也会返回true` |
| `containsValue` | value: Object | `boolean` | 检查指定value是否存在于Map中 | `boolean hasValue = map.containsValue("hdfs://...");` |
| `empty` | 无 | `CaseInsensitiveStringMap` | 创建一个空的CaseInsensitiveStringMap实例 | `CaseInsensitiveStringMap empty = CaseInsensitiveStringMap.empty();
// 返回空的不可变Map` |
| `get` | key: Object | `String` | 获取指定key对应的value（大小写不敏感），不存在返回null | `String path = map.get("path");  // 或"PATH"都可` |
| `getBoolean` | key: String, defaultValue: boolean | `boolean` | 获取指定key的布尔值配置选项，不存在则返回默认值，仅接受"true"/"false"字符串 | `boolean compress = map.getBoolean("compression", false);
// key不存在或无效时返回false` |
| `getDouble` | key: String, defaultValue: double | `double` | 获取指定key的双精度浮点数配置选项，不存在则返回默认值 | `double ratio = map.getDouble("ratio", 1.0);
// 解析字符串为double` |
| `getInt` | key: String, defaultValue: int | `int` | 获取指定key的整数配置选项，不存在则返回默认值 | `int batchSize = map.getInt("batchSize", 1024);
// key不存在时返回1024` |
| `getLong` | key: String, defaultValue: long | `long` | 获取指定key的长整数配置选项，不存在则返回默认值 | `long timeout = map.getLong("timeout", 30000L);` |
| `isEmpty` | 无 | `boolean` | 判断Map是否为空（没有任何键值对） | `if (map.isEmpty()) {
    // Map为空，无配置项
}` |
| `keySet` | 无 | `Set&lt;String&gt;` | 返回所有key的集合（key已转换为小写） | `Set&lt;String&gt; keys = map.keySet();
for (String key : keys) {
    System.out.println(key);  // 输出小写key
}` |
| `put` | key: String, value: String | `String` | 添加键值对（此实现不支持，抛出UnsupportedOperationException） | `// 注意：CaseInsensitiveStringMap是不可变的
// 需要通过构造函数创建` |
| `putAll` | Map&lt;? extends String, ? extends String&gt; | `void` | 批量添加键值对（此实现不支持，抛出UnsupportedOperationException） | `// 注意：不支持修改操作` |
| `remove` | key: Object | `String` | 删除指定key（此实现不支持，抛出UnsupportedOperationException） | `// 注意：不支持删除操作` |
| `size` | 无 | `int` | 返回Map中键值对的数量 | `int count = map.size();
System.out.println("配置项数量: " + count);` |
| `values` | 无 | `Collection&lt;String&gt;` | 返回所有value的集合 | `Collection&lt;String&gt; values = map.values();
for (String value : values) {
    System.out.println(value);
}` |

--------|------|----------|------|------|
| `dataType` | 无 | `DataType` | 获取数据类型 | 返回Cast目标的数据类型 |
| `expression` | 无 | `Expression` | 获取表达式对象 | 返回被转换的表达式对象 |
| `expressionDataType` | 无 | `DataType` | 获取表达式的数据类型 | 返回源表达式的数据类型 |

--------|------|----------|------|------|
| `computeUpdates` | 无 | `boolean` | 是否计算更新操作 | 检查是否计算更新记录 |
| `deduplicationMode` | 无 | `DeduplicationMode` | 获取去重模式 | 返回去重模式配置 |
| `range` | 无 | `ChangelogRange` | 获取变更日志范围 | 返回变更日志的时间范围 |

--------|------|----------|------|------|
| `charTypeWriteSideCheck` | inputStr: UTF8String, limit: int | `UTF8String` | CHAR类型写入端校验，截断超长字符串 | 校验CHAR类型写入，超长则截断 |
| `readSidePadding` | inputStr: UTF8String, limit: int | `UTF8String` | 读取端填充，补齐CHAR类型定长 | 读取端补齐CHAR定长字符串 |
| `varcharTypeWriteSideCheck` | inputStr: UTF8String, limit: int | `UTF8String` | VARCHAR类型写入端校验，截断超长字符串 | 校验VARCHAR类型写入，超长则截断 |


### Check
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `Check` | 构建约束对象 | 构建Check约束对象 |
| `predicate` | 无 | `Predicate` | 获取或设置断言条件 | 获取或设置断言条件 |
| `predicate` | predicate: Predicate | `Builder` | 获取或设置断言条件 | 获取或设置断言条件 |
| `predicateSql` | 无 | `String` | 获取或设置断言SQL表达式 | 获取或设置断言SQL表达式 |
| `predicateSql` | predicateSql: String | `Builder` | 获取或设置断言SQL表达式 | 获取或设置断言SQL表达式 |

--------|------|----------|------|------|
| `binaryTrim` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 二进制模式去除两端指定字符 | 去除字符串两端指定字符（二进制模式） |
| `binaryTrimRight` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 二进制模式去除右侧指定字符 | 去除字符串右侧指定字符（二进制模式） |
| `compareLowerCase` | left: final UTF8String, right: final UTF8String | `int` | 比较两个字符串的小写形式 | 比较两字符串小写形式，返回差值 |
| `findInSet` | match: final UTF8String, set: final UTF8String, collationId: int | `int` | 在集合字符串中查找匹配项位置 | 在逗号分隔集合中查找元素位置 |
| `indexOf` | target: final UTF8String, pattern: final UTF8String, start: final int, collationId: final int | `int` | 查找子串在字符串中的起始位置 | 查找子串起始位置，支持指定起始索引 |
| `lowerCaseCodePoints` | target: final UTF8String | `UTF8String` | 获取小写形式的Unicode码点 | 获取小写Unicode码点字符串 |
| `lowercaseContains` | target: final UTF8String, pattern: final UTF8String | `boolean` | 忽略大小写检查是否包含子串 | 忽略大小写检查是否包含子串 |
| `lowercaseEndsWith` | target: final UTF8String, pattern: final UTF8String | `boolean` | 忽略大小写检查是否以指定字符串结尾 | 忽略大小写检查是否以指定结尾 |
| `lowercaseIndexOf` | target: final UTF8String, pattern: final UTF8String, start: final int | `int` | 忽略大小写查找子串位置 | 忽略大小写查找子串位置 |
| `lowercaseReplace` | target: final UTF8String, search: final UTF8String, replace: final UTF8String | `UTF8String` | 忽略大小写替换匹配的字符串 | 忽略大小写替换匹配内容 |
| `lowercaseStartsWith` | target: final UTF8String, pattern: final UTF8String | `boolean` | 忽略大小写检查是否以指定字符串开头 | 忽略大小写检查是否以指定开头 |
| `lowercaseSubStringIndex` | string: final UTF8String, delimiter: final UTF8String, count: int | `UTF8String` | 忽略大小写的子串索引查找 | 忽略大小写的子串索引 |
| `lowercaseTranslate` | input: final UTF8String, Map<String: final, dict: String> | `UTF8String` | 忽略大小写的字符转换 | 忽略大小写的字符映射转换 |
| `lowercaseTrim` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 忽略大小写去除两端空白 | 忽略大小写去除两端空白 |
| `lowercaseTrimLeft` | srcString: final UTF8String, trimString: final UTF8String | `UTF8String` | 忽略大小写去除左侧空白 | 忽略大小写去除左侧空白 |
| `lowercaseTrimRight` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 忽略大小写去除右侧空白 | 忽略大小写去除右侧空白 |
| `replace` | target: final UTF8String, search: final UTF8String, replace: final UTF8String, collationId: final int | `UTF8String` | 替换字符串中匹配的内容 | 替换字符串中匹配内容 |
| `subStringIndex` | string: final UTF8String, delimiter: final UTF8String, count: int, collationId: final int | `UTF8String` | 查找分隔符分隔的子串索引 | 按分隔符查找第N个子串 |
| `toLowerCase` | target: final UTF8String | `UTF8String` | 转换为小写 | 转换为小写字符串 |
| `toLowerCase` | target: final UTF8String, collationId: final int | `UTF8String` | 转换为小写 | 转换为小写字符串 |
| `toTitleCase` | target: final UTF8String | `UTF8String` | 转换为标题大小写 | 转换为标题大小写（首字母大写） |
| `toTitleCase` | target: final UTF8String, collationId: final int | `UTF8String` | 转换为标题大小写 | 转换为标题大小写（首字母大写） |
| `toTitleCaseICU` | source: UTF8String | `UTF8String` | 使用ICU库转换为标题大小写 | ICU库标题大小写转换 |
| `toUpperCase` | target: final UTF8String | `UTF8String` | 转换为大写 | 转换为大写字符串 |
| `toUpperCase` | target: final UTF8String, collationId: final int | `UTF8String` | 转换为大写 | 转换为大写字符串 |
| `translate` | input: final UTF8String, Map<String: final, dict: String>, collationId: final int | `UTF8String` | 字符映射转换 | 按字符映射表转换字符串 |
| `trim` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 去除空白 | 去除字符串两端空白 |
| `trimLeft` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 去除字符串左侧空白 | 去除字符串左侧空白 |
| `trimRight` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 去除字符串右侧空白 | 去除字符串右侧空白 |

--------|------|----------|------|------|
| `getValue` | 无 | `Literal&lt;?&gt;` | 获取列的默认值 | 返回列默认值的Literal对象 |


### CustomAvgMetric
**包路径**: `org.apache.spark.sql.connector.metric`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `aggregateTaskMetrics` | taskMetrics: long&lt;&gt; | `String` | 聚合任务级别的度量指标 | 聚合任务度量指标为字符串 |


### CustomSumMetric
**包路径**: `org.apache.spark.sql.connector.metric`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `aggregateTaskMetrics` | taskMetrics: long&lt;&gt; | `String` | 聚合任务级别的度量指标 | 聚合任务度量指标为字符串 |


### CustomTaskMetric
**包路径**: `org.apache.spark.sql.connector.metric`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |
| `value` | 无 | `long` | 获取度量指标值 | 返回度量指标数值 |


### Distributions
**包路径**: `org.apache.spark.sql.connector.distributions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `clustered` | clustering: Expression&lt;&gt; | `ClusteredDistribution` | 创建聚类分布 | 创建聚类分布对象 |
| `ordered` | ordering: SortOrder&lt;&gt; | `OrderedDistribution` | 创建有序分布 | 传入参数执行创建有序分布 |
| `unspecified` | 无 | `UnspecifiedDistribution` | 创建未指定分布 | 调用该方法执行创建未指定分布 |

--------|------|----------|------|------|
| `getSentences` | str: UTF8String, language: UTF8String, country: UTF8String | `ArrayData` | 将文本分割为句子数组 | 传入参数执行将文本分割为句子数组 |
| `getSparkVersion` | 无 | `UTF8String` | 获取Spark版本字符串 | 调用该方法执行获取Spark版本字符串 |
| `isLuhnNumber` | numberString: UTF8String | `boolean` | 校验Luhn算法数字（信用卡号校验） | 传入参数执行校验Luhn算法数字（信用卡号校验） |
| `quote` | str: UTF8String | `UTF8String` | 对字符串进行引用处理 | 传入参数执行对字符串进行引用处理 |
| `randStr` | rng: XORShiftRandom, length: int | `UTF8String` | 生成随机字符串 | 传入参数执行生成随机字符串 |
| `tryValidateUTF8String` | utf8String: UTF8String | `UTF8String` | 尝试校验UTF8字符串 | 传入参数执行尝试校验UTF8字符串 |
| `validateUTF8String` | utf8String: UTF8String | `UTF8String` | 校验UTF8字符串有效性 | 传入参数执行校验UTF8字符串有效性 |

--------|------|----------|------|------|
| `getArguments` | 无 | `String` | 获取函数参数说明 | 调用该方法执行获取函数参数说明 |
| `getClassName` | 无 | `String` | 获取类名 | 调用该方法执行获取类名 |
| `getDb` | 无 | `String` | 获取数据库名 | 调用该方法执行获取数据库名 |
| `getDeprecated` | 无 | `String` | 获取弃用说明 | 调用该方法执行获取弃用说明 |
| `getExamples` | 无 | `String` | 获取使用示例 | 调用该方法执行获取使用示例 |
| `getExtended` | 无 | `String` | 获取扩展说明 | 调用该方法执行获取扩展说明 |
| `getGroup` | 无 | `String` | 获取函数分组 | 调用该方法执行获取函数分组 |
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `getNote` | 无 | `String` | 获取备注说明 | 调用该方法执行获取备注说明 |
| `getOriginalExamples` | 无 | `String` | 获取原始示例 | 调用该方法执行获取原始示例 |
| `getSince` | 无 | `String` | 获取版本信息 | 调用该方法执行获取版本信息 |
| `getSource` | 无 | `String` | 获取来源 | 调用该方法执行获取来源 |
| `getUsage` | 无 | `String` | 获取使用说明 | 调用该方法执行获取使用说明 |


### Expressions
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | name: String, args: Expression... | `Transform` | 应用数据类型转换 | 获取数据类型对应的列向量 |
| `bucket` | numBuckets: int, columns: String... | `Transform` | 创建分桶分区转换 | 传入参数执行创建分桶分区转换 |
| `column` | name: String | `NamedReference` | 创建列引用表达式 | 传入参数执行创建列引用表达式 |
| `days` | column: String | `Transform` | 将日期转换为天数 | 传入参数执行将日期转换为天数 |
| `hours` | column: String | `Transform` | 将时间转换为小时数 | 传入参数执行将时间转换为小时数 |
| `identity` | column: String | `Transform` | 创建身份分区转换 | 传入参数执行创建身份分区转换 |
| `months` | column: String | `Transform` | 将日期转换为月份数 | 传入参数执行将日期转换为月份数 |
| `sort` | expr: Expression, direction: SortDirection, nullOrder: NullOrdering | `SortOrder` | 排序 | 传入参数执行创建排序表达式 |
| `sort` | expr: Expression, direction: SortDirection | `SortOrder` | 排序 | 传入参数执行创建排序表达式 |
| `years` | column: String | `Transform` | 年份转换相关功能 | 传入参数执行年份转换相关功能 |


### ForeignKey
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `ForeignKey` | 构建约束对象 | 构建Check约束对象 |
| `referencedTable` | 无 | `Identifier` | 引用encedTable相关功能 | 调用该方法执行引用encedTable相关功能 |


### GeneralScalarExpression
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |

--------|------|----------|------|------|
| `getSrid` | stringId: String | `Integer` | 获取空间参考系统ID（SRID） | 将字符串空间参考ID转换为整数SRID |
| `getStringId` | srid: int | `String` | 将SRID转换为字符串标识 | 将整数SRID转换为字符串标识 |

--------|------|----------|------|------|
| `toWkt` | 无 | `String` | toWkt操作 | 调用该方法执行toWkt操作 |


### GetArrayItem
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `childArray` | 无 | `Expression` | 子级Array相关功能 | 调用该方法执行子级Array相关功能 |
| `failOnError` | 无 | `boolean` | failOnError操作 | 调用该方法执行failOnError操作 |
| `ordinal` | 无 | `Expression` | ordinal操作 | 调用该方法执行ordinal操作 |

--------|------|----------|------|------|
| `getCompressionCodec` | 无 | `CompressionCodec` | 获取CompressionCodec相关功能 | 调用该方法执行获取CompressionCodec相关功能 |
| `lowerCaseName` | 无 | `String` | 转换为小写的名称 | 返回编解码器名称的小写形式 |

--------|------|----------|------|------|
| `getCurrentKey` | 无 | `LongWritable` | 获取CurrentKey相关功能 | 调用该方法执行获取CurrentKey相关功能 |
| `getCurrentValue` | 无 | `Text` | 获取CurrentValue相关功能 | 调用该方法执行获取CurrentValue相关功能 |
| `getProgress` | 无 | `float` | 获取Progress相关功能 | 调用该方法执行获取Progress相关功能 |
| `initialize` | genericSplit: InputSplit, context: TaskAttemptContext | `void` | 初始化插件 | 初始化目录插件 |
| `nextKeyValue` | 无 | `boolean` | 之后KeyValue相关功能 | 调用该方法执行之后KeyValue相关功能 |

--------|------|----------|------|------|
| `hashInt` | input: int | `int` | 检查是否存在hInt相关功能 | 传入参数执行检查是否存在hInt相关功能 |
| `hashLong` | input: long | `int` | 检查是否存在hLong相关功能 | 传入参数执行检查是否存在hLong相关功能 |
| `hashUnsafeBytes` | base: Object, offset: long, lengthInBytes: int | `int` | 检查是否存在hUnsafeBytes相关功能 | 传入参数执行检查是否存在hUnsafeBytes相关功能 |


### IdentityColumnSpec
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStart` | 无 | `long` | 获取Start相关功能 | 调用该方法执行获取Start相关功能 |
| `getStep` | 无 | `long` | 获取Step相关功能 | 调用该方法执行获取Step相关功能 |
| `isAllowExplicitInsert` | 无 | `boolean` | 判断是否AllowExplicitInsert相关功能 | 调用该方法执行判断是否AllowExplicitInsert相关功能 |


### IntegerAdd
**包路径**: `org.apache.spark.sql.connector.catalog.functions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `invoke` | left: int, right: int | `int` | 调用相关功能 | 传入参数执行调用相关功能 |
| `produceResult` | input: InternalRow | `Integer` | 生产Result相关功能 | 传入参数执行生产Result相关功能 |

--------|------|----------|------|------|
| `getCube` | 无 | `int` | 获取Cube相关功能 | 调用该方法执行获取Cube相关功能 |
| `getSquare` | 无 | `int` | 获取Square相关功能 | 调用该方法执行获取Square相关功能 |
| `getValue` | 无 | `int` | 获取列的默认值 | 返回列默认值的Literal对象 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `setCube` | cube: int | `void` | 设置Cube相关功能 | 传入参数执行设置Cube相关功能 |
| `setSquare` | square: int | `void` | 设置Square相关功能 | 传入参数执行设置Square相关功能 |
| `setValue` | value: int | `void` | 设置Value相关功能 | 传入参数执行设置Value相关功能 |

--------|------|----------|------|------|
| `getKey` | 无 | `int` | 获取Key相关功能 | 调用该方法执行获取Key相关功能 |
| `getValue` | 无 | `String` | 获取列的默认值 | 返回列默认值的Literal对象 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `setKey` | key: int | `void` | 设置Key相关功能 | 传入参数执行设置Key相关功能 |
| `setValue` | value: String | `void` | 设置Value相关功能 | 传入参数执行设置Value相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `getAge` | 无 | `long` | 获取Age相关功能 | 调用该方法执行获取Age相关功能 |
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `setAge` | age: long | `void` | 设置Age相关功能 | 传入参数执行设置Age相关功能 |
| `setName` | name: String | `void` | 设置RDD名称 | 传入参数执行设置Name相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `bufferEncoder` | 无 | `Encoder&lt;Average&gt;` | 编码相关功能 | 调用该方法执行编码相关功能 |
| `finish` | reduction: Average | `Double` | 完成相关功能 | 传入参数执行完成相关功能 |
| `getCount` | 无 | `long` | 获取Count相关功能 | 调用该方法执行获取Count相关功能 |
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `getSalary` | 无 | `long` | 获取Salary相关功能 | 调用该方法执行获取Salary相关功能 |
| `getSum` | 无 | `long` | 获取Sum相关功能 | 调用该方法执行获取Sum相关功能 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `merge` | b1: Average, b2: Average | `Average` | 合并相关功能 | 传入参数执行合并相关功能 |
| `outputEncoder` | 无 | `Encoder&lt;Double&gt;` | 编码相关功能 | 调用该方法执行编码相关功能 |
| `reduce` | buffer: Average, employee: Employee | `Average` | 聚合DStream每个RDD | // reduce：聚合所有元素为单个结果
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

// 求和
Integer sum = numbers.reduce((a, b) -> a + b);
// 结果: 15

// 求最大值
Integer max = numbers.reduce((a, b) -> Math.max(a, b));
// 结果: 5

// 字符串拼接
JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"));
String concatenated = words.reduce((a, b) -> a + b);
// 结果: "abc" |
| `setCount` | count: long | `void` | 设置Count相关功能 | 传入参数执行设置Count相关功能 |
| `setName` | name: String | `void` | 设置RDD名称 | 传入参数执行设置Name相关功能 |
| `setSalary` | salary: long | `void` | 设置Salary相关功能 | 传入参数执行设置Salary相关功能 |
| `setSum` | sum: long | `void` | 设置Sum相关功能 | 传入参数执行设置Sum相关功能 |
| `zero` | 无 | `Average` | zero操作 | 调用该方法执行zero操作 |

--------|------|----------|------|------|
| `bufferEncoder` | 无 | `Encoder&lt;Average&gt;` | 编码相关功能 | 调用该方法执行编码相关功能 |
| `finish` | reduction: Average | `Double` | 完成相关功能 | 传入参数执行完成相关功能 |
| `getCount` | 无 | `long` | 获取Count相关功能 | 调用该方法执行获取Count相关功能 |
| `getSum` | 无 | `long` | 获取Sum相关功能 | 调用该方法执行获取Sum相关功能 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `merge` | b1: Average, b2: Average | `Average` | 合并相关功能 | 传入参数执行合并相关功能 |
| `outputEncoder` | 无 | `Encoder&lt;Double&gt;` | 编码相关功能 | 调用该方法执行编码相关功能 |
| `reduce` | buffer: Average, data: Long | `Average` | 聚合DStream每个RDD | // reduce：聚合所有元素为单个结果
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

// 求和
Integer sum = numbers.reduce((a, b) -> a + b);
// 结果: 15

// 求最大值
Integer max = numbers.reduce((a, b) -> Math.max(a, b));
// 结果: 5

// 字符串拼接
JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"));
String concatenated = words.reduce((a, b) -> a + b);
// 结果: "abc" |
| `setCount` | count: long | `void` | 设置Count相关功能 | 传入参数执行设置Count相关功能 |
| `setSum` | sum: long | `void` | 设置Sum相关功能 | 传入参数执行设置Sum相关功能 |
| `zero` | 无 | `Average` | zero操作 | 调用该方法执行zero操作 |

--------|------|----------|------|------|
| `jsonObjectKeys` | json: UTF8String | `GenericArrayData` | jsonObjectKeys操作 | 传入参数执行jsonObjectKeys操作 |
| `lengthOfJsonArray` | json: UTF8String | `Integer` | lengthOfJsonArray操作 | 传入参数执行lengthOfJsonArray操作 |

--------|------|----------|------|------|
| `cleanupResources` | 无 | `void` | 向上相关功能 | 调用该方法执行向上相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `compare` | baseObj1: Object, baseOff1: long, baseLen1: int, baseObj2: Object, baseOff2: long, baseLen2: int | `int` | 比较相关功能 | 传入参数执行比较相关功能 |
| `getKey` | 无 | `UnsafeRow` | 获取Key相关功能 | 调用该方法执行获取Key相关功能 |
| `getPeakMemoryUsedBytes` | 无 | `long` | 获取PeakMemoryUsedBytes相关功能 | 调用该方法执行获取PeakMemoryUsedBytes相关功能 |
| `getSpillSize` | 无 | `long` | 获取SpillSize相关功能 | 调用该方法执行获取SpillSize相关功能 |
| `getValue` | 无 | `UnsafeRow` | 获取列的默认值 | 返回列默认值的Literal对象 |
| `insertKV` | key: UnsafeRow, value: UnsafeRow | `void` | 插入KV相关功能 | 传入参数执行插入KV相关功能 |
| `merge` | other: UnsafeKVExternalSorter | `void` | 合并相关功能 | 传入参数执行合并相关功能 |
| `next` | 无 | `boolean` | 获取迭代器的下一个元素 | 获取迭代器下一行数据 |
| `sortedIterator` | 无 | `KVSorterIterator` | 排序edIterator相关功能 | 调用该方法执行排序edIterator相关功能 |


### KeyGroupedPartitioning
**包路径**: `org.apache.spark.sql.connector.read.partitioning`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `numPartitions` | 无 | `int` | numPartitions操作 | 调用该方法执行numPartitions操作 |


### NamespaceChange
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `property` | 无 | `String` | property操作 | 调用该方法执行property操作 |
| `value` | 无 | `String` | 获取度量指标值 | 返回度量指标数值 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |


### NumericHistogram
**包路径**: `org.apache.spark.sql.util`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | v: double | `void` | 添加元素 | 传入参数执行添加相关功能 |
| `addBin` | x: double, y: double, b: int | `void` | 添加二进制数据 | 传入参数执行添加二进制数据 |
| `allocate` | num_bins: int | `void` | 分配相关功能 | 传入参数执行分配相关功能 |
| `compareTo` | other: Coord | `int` | 比较To相关功能 | 传入参数执行比较To相关功能 |
| `getBin` | b: int | `Coord` | 获取Bin相关功能 | 传入参数执行获取Bin相关功能 |
| `getNumBins` | 无 | `int` | 获取NumBins相关功能 | 调用该方法执行获取NumBins相关功能 |
| `getUsedBins` | 无 | `int` | 获取UsedBins相关功能 | 调用该方法执行获取UsedBins相关功能 |
| `isReady` | 无 | `boolean` | 判断是否Ready相关功能 | 调用该方法执行判断是否Ready相关功能 |
| `merge` | other: NumericHistogram | `void` | 合并相关功能 | 传入参数执行合并相关功能 |
| `reset` | 无 | `void` | 重置相关功能 | 调用该方法执行重置相关功能 |
| `setUsedBins` | nusedBins: int | `void` | 设置UsedBins相关功能 | 传入参数执行设置UsedBins相关功能 |

--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | 获取Array相关功能 | 传入参数执行获取Array相关功能 |
| `getBoolean` | rowId: int | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getByte` | rowId: int | `byte` | 获取Byte相关功能 | 传入参数执行获取Byte相关功能 |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal相关功能 | 传入参数执行获取Decimal相关功能 |
| `getDouble` | rowId: int | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getFloat` | rowId: int | `float` | 获取Float相关功能 | 传入参数执行获取Float相关功能 |
| `getInt` | rowId: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getLong` | rowId: int | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |
| `getMap` | rowId: int | `ColumnarMap` | 获取Map相关功能 | 传入参数执行获取Map相关功能 |
| `getShort` | rowId: int | `short` | 获取Short相关功能 | 传入参数执行获取Short相关功能 |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8String相关功能 | 传入参数执行获取UTF8String相关功能 |

--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | 获取Array相关功能 | 传入参数执行获取Array相关功能 |
| `getBoolean` | rowId: int | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getByte` | rowId: int | `byte` | 获取Byte相关功能 | 传入参数执行获取Byte相关功能 |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal相关功能 | 传入参数执行获取Decimal相关功能 |
| `getDouble` | rowId: int | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getFloat` | rowId: int | `float` | 获取Float相关功能 | 传入参数执行获取Float相关功能 |
| `getInt` | rowId: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getLong` | rowId: int | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |
| `getMap` | rowId: int | `ColumnarMap` | 获取Map相关功能 | 传入参数执行获取Map相关功能 |
| `getShort` | rowId: int | `short` | 获取Short相关功能 | 传入参数执行获取Short相关功能 |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8String相关功能 | 传入参数执行获取UTF8String相关功能 |

--------|------|----------|------|------|
| `add` | newChild: OrcColumnStatistics | `void` | 添加元素 | 传入参数执行添加相关功能 |
| `get` | ordinal: int | `OrcColumnStatistics` | 获取元素 | 传入参数执行获取相关功能 |
| `getStatistics` | 无 | `ColumnStatistics` | 获取Statistics相关功能 | 调用该方法执行获取Statistics相关功能 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `hasNull` | 无 | `boolean` | 检查是否存在Null相关功能 | 调用该方法执行检查是否存在Null相关功能 |
| `isNullAt` | rowId: int | `boolean` | 判断是否NullAt相关功能 | 传入参数执行判断是否NullAt相关功能 |
| `numNulls` | 无 | `int` | numNulls操作 | 调用该方法执行numNulls操作 |
| `setBatchSize` | batchSize: int | `void` | 设置BatchSize相关功能 | 传入参数执行设置BatchSize相关功能 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getCurrentKey` | 无 | `Void` | 获取CurrentKey相关功能 | 调用该方法执行获取CurrentKey相关功能 |
| `getCurrentValue` | 无 | `ColumnarBatch` | 获取CurrentValue相关功能 | 调用该方法执行获取CurrentValue相关功能 |
| `getProgress` | 无 | `float` | 获取Progress相关功能 | 调用该方法执行获取Progress相关功能 |
| `initBatch` | orcSchema: TypeDescription, requiredFields: StructField&lt;&gt;, requestedDataColIds: int&lt;&gt;, requestedPartitionColIds: int&lt;&gt;, partitionValues: InternalRow | `void` | 初始化Batch相关功能 | 传入参数执行初始化Batch相关功能 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext, orcTail: OrcTail | `void` | 初始化插件 | 初始化目录插件 |
| `nextKeyValue` | 无 | `boolean` | 之后KeyValue相关功能 | 调用该方法执行之后KeyValue相关功能 |

--------|------|----------|------|------|
| `getCompressionKind` | 无 | `CompressionKind` | 获取CompressionKind相关功能 | 调用该方法执行获取CompressionKind相关功能 |
| `lowerCaseName` | 无 | `String` | 转换为小写的名称 | 返回编解码器名称的小写形式 |

--------|------|----------|------|------|
| `readStatistics` | orcReader: Reader | `OrcColumnStatistics` | 读取Statistics相关功能 | 传入参数执行读取Statistics相关功能 |

--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | 获取Array相关功能 | 传入参数执行获取Array相关功能 |
| `getBoolean` | rowId: int | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getByte` | rowId: int | `byte` | 获取Byte相关功能 | 传入参数执行获取Byte相关功能 |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal相关功能 | 传入参数执行获取Decimal相关功能 |
| `getDouble` | rowId: int | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getFloat` | rowId: int | `float` | 获取Float相关功能 | 传入参数执行获取Float相关功能 |
| `getInt` | rowId: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getLong` | rowId: int | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |
| `getMap` | ordinal: int | `ColumnarMap` | 获取Map相关功能 | 传入参数执行获取Map相关功能 |
| `getShort` | rowId: int | `short` | 获取Short相关功能 | 传入参数执行获取Short相关功能 |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8String相关功能 | 传入参数执行获取UTF8String相关功能 |

--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | 获取Array相关功能 | 传入参数执行获取Array相关功能 |
| `getBoolean` | rowId: int | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getByte` | rowId: int | `byte` | 获取Byte相关功能 | 传入参数执行获取Byte相关功能 |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal相关功能 | 传入参数执行获取Decimal相关功能 |
| `getDouble` | rowId: int | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getFloat` | rowId: int | `float` | 获取Float相关功能 | 传入参数执行获取Float相关功能 |
| `getInt` | rowId: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getLong` | rowId: int | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |
| `getMap` | rowId: int | `ColumnarMap` | 获取Map相关功能 | 传入参数执行获取Map相关功能 |
| `getShort` | rowId: int | `short` | 获取Short相关功能 | 传入参数执行获取Short相关功能 |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8String相关功能 | 传入参数执行获取UTF8String相关功能 |

--------|------|----------|------|------|
| `fromString` | s: String | `ParquetCompressionCodec` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |
| `getCompressionCodec` | 无 | `CompressionCodecName` | 获取CompressionCodec相关功能 | 调用该方法执行获取CompressionCodec相关功能 |
| `lowerCaseName` | 无 | `String` | 转换为小写的名称 | 返回编解码器名称的小写形式 |

--------|------|----------|------|------|
| `openFileAndReadFooter` | hadoopConf: Configuration, file: PartitionedFile, keepInputStreamOpen: boolean | `OpenedParquetFooter` | 打开FileAndReadFooter相关功能 | 传入参数执行打开FileAndReadFooter相关功能 |
| `readFooter` | inputFile: HadoopInputFile, filter: ParquetMetadataConverter.MetadataFilter | `ParquetMetadata` | 读取Footer相关功能 | 传入参数执行读取Footer相关功能 |

--------|------|----------|------|------|
| `decodeSingleDictionaryId` | offset: int, values: WritableColumnVector, dictionaryIds: WritableColumnVector, dictionary: Dictionary | `void` | 解码SingleDictionaryId相关功能 | 传入参数执行解码SingleDictionaryId相关功能 |
| `getUpdater` | descriptor: ColumnDescriptor, sparkType: DataType | `ParquetVectorUpdater` | 获取Updater相关功能 | 传入参数执行获取Updater相关功能 |
| `readValue` | offset: int, values: WritableColumnVector, valuesReader: VectorizedValuesReader | `void` | 读取Value相关功能 | 传入参数执行读取Value相关功能 |
| `readValues` | total: int, offset: int, values: WritableColumnVector, valuesReader: VectorizedValuesReader | `void` | 读取Values相关功能 | 传入参数执行读取Values相关功能 |
| `skipValues` | total: int, valuesReader: VectorizedValuesReader | `void` | 跳过Values相关功能 | 传入参数执行跳过Values相关功能 |


### PrimaryKey
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `PrimaryKey` | 构建约束对象 | 构建Check约束对象 |


### ProcedureParameter
**包路径**: `org.apache.spark.sql.connector.catalog.procedures`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `ProcedureParameter` | 构建约束对象 | 构建Check约束对象 |
| `comment` | comment: String | `Builder` | comment操作 | 传入参数执行comment操作 |
| `defaultValue` | sql: String | `Builder` | 默认Value相关功能 | 传入参数执行默认Value相关功能 |
| `defaultValue` | expression: Expression | `Builder` | 默认Value相关功能 | 传入参数执行默认Value相关功能 |
| `defaultValue` | defaultValue: DefaultValue | `Builder` | 默认Value相关功能 | 传入参数执行默认Value相关功能 |

--------|------|----------|------|------|
| `allocate` | keySchema: StructType, valueSchema: StructType, manager: TaskMemoryManager | `RowBasedKeyValueBatch` | 分配相关功能 | 传入参数执行分配相关功能 |
| `allocate` | keySchema: StructType, valueSchema: StructType, manager: TaskMemoryManager, maxRows: int | `RowBasedKeyValueBatch` | 分配相关功能 | 传入参数执行分配相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getValueRow` | rowId: int | `UnsafeRow` | 获取ValueRow相关功能 | 传入参数执行获取ValueRow相关功能 |
| `numRows` | 无 | `int` | numRows操作 | 调用该方法执行numRows操作 |
| `spill` | size: long, trigger: MemoryConsumer | `long` | spill操作 | 传入参数执行spill操作 |


### SortDirection
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultNullOrdering` | 无 | `NullOrdering` | 默认NullOrdering相关功能 | 调用该方法执行默认NullOrdering相关功能 |

--------|------|----------|------|------|
| `getInstance` | 无 | `SpatialReferenceSystemCache` | 获取Instance相关功能 | 调用该方法执行获取Instance相关功能 |
| `getSridToSrs` | 无 | `Map&lt;Integer, SpatialReferenceSystemInformation&gt;` | 获取SridToSrs相关功能 | 调用该方法执行获取SridToSrs相关功能 |
| `getSrsInfo` | srid: int | `SpatialReferenceSystemInformation` | 获取SrsInfo相关功能 | 传入参数执行获取SrsInfo相关功能 |
| `getSrsInfo` | stringId: String | `SpatialReferenceSystemInformation` | 获取SrsInfo相关功能 | 传入参数执行获取SrsInfo相关功能 |
| `getStringIdToSrs` | 无 | `Map&lt;String, SpatialReferenceSystemInformation&gt;` | 获取StringIdToSrs相关功能 | 调用该方法执行获取StringIdToSrs相关功能 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getCurrentKey` | 无 | `Void` | 获取CurrentKey相关功能 | 调用该方法执行获取CurrentKey相关功能 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext, inputFile: Option<HadoopInputFile>, inputStream: Option<SeekableInputStream>, fileFooter: Option<ParquetMetadata> | `void` | 初始化插件 | 初始化目录插件 |
| `readNextRowGroup` | 无 | `PageReadStore` | 读取NextRowGroup相关功能 | 调用该方法执行读取NextRowGroup相关功能 |


### SupportsPushDownJoin
**包路径**: `org.apache.spark.sql.connector.read`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `prettyString` | 无 | `String` | 前ttyString相关功能 | 调用该方法执行前ttyString相关功能 |


### TableChange
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `column` | 无 | `String` | 创建列引用表达式 | 调用该方法执行创建列引用表达式 |
| `comment` | 无 | `String` | comment操作 | 调用该方法执行comment操作 |
| `constraint` | 无 | `Constraint` | 约束相关功能 | 调用该方法执行约束相关功能 |
| `dataType` | 无 | `DataType` | 获取数据类型 | 返回Cast目标的数据类型 |
| `defaultValue` | 无 | `ColumnDefaultValue` | 默认Value相关功能 | 调用该方法执行默认Value相关功能 |
| `ifExists` | 无 | `Boolean` | 判断是否相关功能 | 调用该方法执行判断是否相关功能 |
| `ifExists` | 无 | `boolean` | 判断是否相关功能 | 调用该方法执行判断是否相关功能 |
| `isNullable` | 无 | `boolean` | 判断是否Nullable相关功能 | 调用该方法执行判断是否Nullable相关功能 |
| `mode` | 无 | `Mode` | mode操作 | 调用该方法执行mode操作 |
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |
| `newComment` | 无 | `String` | newComment操作 | 调用该方法执行newComment操作 |
| `newCurrentDefault` | 无 | `DefaultValue` | 默认相关功能 | 调用该方法执行默认相关功能 |
| `newDataType` | 无 | `DataType` | newDataType操作 | 调用该方法执行newDataType操作 |
| `newDefaultValue` | 无 | `String` | 默认相关功能 | 调用该方法执行默认相关功能 |
| `newName` | 无 | `String` | newName操作 | 调用该方法执行newName操作 |
| `nullable` | 无 | `boolean` | nullable操作 | 调用该方法执行nullable操作 |
| `position` | 无 | `ColumnPosition` | position操作 | 调用该方法执行position操作 |
| `property` | 无 | `String` | property操作 | 调用该方法执行property操作 |
| `validatedTableVersion` | 无 | `String` | 校验dTableVersion相关功能 | 调用该方法执行校验dTableVersion相关功能 |
| `value` | 无 | `String` | 获取度量指标值 | 返回度量指标数值 |


### TableInfo
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `TableInfo` | 构建约束对象 | 构建Check约束对象 |
| `properties` | 无 | `Map&lt;String, String&gt;` | properties操作 | 调用该方法执行properties操作 |
| `schema` | 无 | `StructType` | 获取schema | 调用该方法执行schema操作 |
| `withColumns` | columns: Column&lt;&gt; | `Builder` | 列相关功能 | 传入参数执行列相关功能 |
| `withConstraints` | constraints: Constraint&lt;&gt; | `Builder` | 约束相关功能 | 传入参数执行约束相关功能 |
| `withPartitions` | partitions: Transform&lt;&gt; | `Builder` | withPartitions操作 | 传入参数执行withPartitions操作 |
| `withProperties` | properties: String> | `Builder` | withProperties操作 | 传入参数执行withProperties操作 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `eval` | xml: String, path: String, qname: QName | `Object` | eval操作 | 传入参数执行eval操作 |
| `evalBoolean` | xml: String, path: String | `Boolean` | evalBoolean操作 | 传入参数执行evalBoolean操作 |
| `evalNode` | xml: String, path: String | `Node` | evalNode操作 | 传入参数执行evalNode操作 |
| `evalNodeList` | xml: String, path: String | `NodeList` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `evalNumber` | xml: String, path: String | `Double` | evalNumber操作 | 传入参数执行evalNumber操作 |
| `evalString` | xml: String, path: String | `String` | 三相关功能 | 传入参数执行三相关功能 |
| `mark` | readAheadLimit: int | `void` | mark操作 | 传入参数执行mark操作 |
| `markSupported` | 无 | `boolean` | 支持相关功能 | 调用该方法执行支持相关功能 |
| `read` | 无 | `int` | 读取数据源创建DataFrame | 调用该方法执行读取相关功能 |
| `read` | cbuf: char&lt;&gt;, off: int, len: int | `int` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |
| `ready` | 无 | `boolean` | 读取y相关功能 | 调用该方法执行读取y相关功能 |
| `reset` | 无 | `void` | 重置相关功能 | 调用该方法执行重置相关功能 |
| `set` | s: String | `void` | 设置元素 | 传入参数执行设置相关功能 |
| `skip` | ns: long | `long` | 跳过相关功能 | 传入参数执行跳过相关功能 |


### Unique
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `Unique` | 构建约束对象 | 构建Check约束对象 |


### UnknownPartitioning
**包路径**: `org.apache.spark.sql.connector.read.partitioning`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `numPartitions` | 无 | `int` | numPartitions操作 | 调用该方法执行numPartitions操作 |

--------|------|----------|------|------|
| `cursor` | 无 | `int` | cursor操作 | 调用该方法执行cursor操作 |
| `getBufferHolder` | 无 | `BufferHolder` | 获取BufferHolder相关功能 | 调用该方法执行获取BufferHolder相关功能 |
| `grow` | neededSize: int | `void` | grow操作 | 传入参数执行grow操作 |
| `increaseCursor` | val: int | `void` | increaseCursor操作 | 传入参数执行increaseCursor操作 |
| `reset` | 无 | `void` | 重置相关功能 | 调用该方法执行重置相关功能 |
| `setOffsetAndSizeFromPreviousCursor` | ordinal: int, previousCursor: int | `void` | 设置OffsetAndSizeFromPreviousCursor相关功能 | 传入参数执行设置OffsetAndSizeFromPreviousCursor相关功能 |
| `totalSize` | 无 | `int` | totalSize操作 | 调用该方法执行totalSize操作 |
| `write` | ordinal: int, input: UTF8String | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: GeographyVal | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: GeometryVal | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: byte&lt;&gt; | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: byte&lt;&gt;, offset: int, numBytes: int | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: CalendarInterval | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: VariantVal | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, row: UnsafeRow | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, map: UnsafeMapData | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | array: UnsafeArrayData | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |


### UserDefinedAggregateFunc
**包路径**: `org.apache.spark.sql.connector.expressions.aggregate`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `canonicalName` | 无 | `String` | 判断能否onicalName相关功能 | 调用该方法执行判断能否onicalName相关功能 |
| `isDistinct` | 无 | `boolean` | 判断是否Distinct相关功能 | 调用该方法执行判断是否Distinct相关功能 |
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |


### UserDefinedScalarFunc
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `canonicalName` | 无 | `String` | 判断能否onicalName相关功能 | 调用该方法执行判断能否onicalName相关功能 |
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |


### V2ExpressionSQLBuilder
**包路径**: `org.apache.spark.sql.connector.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | expr: Expression | `String` | 构建约束对象 | 构建Check约束对象 |

--------|------|----------|------|------|
| `vectorCosineSimilarity` | left: ArrayData, right: ArrayData, funcName: UTF8String | `Float` | vectorCosineSimilarity操作 | 传入参数执行vectorCosineSimilarity操作 |
| `vectorInfNorm` | vec: ArrayData | `Float` | vectorInfNorm操作 | 传入参数执行vectorInfNorm操作 |
| `vectorInnerProduct` | left: ArrayData, right: ArrayData, funcName: UTF8String | `Float` | vectorInnerProduct操作 | 传入参数执行vectorInnerProduct操作 |
| `vectorL1Norm` | vec: ArrayData | `Float` | vectorL1Norm操作 | 传入参数执行vectorL1Norm操作 |
| `vectorL2Distance` | left: ArrayData, right: ArrayData, funcName: UTF8String | `Float` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `vectorL2Norm` | vec: ArrayData | `Float` | vectorL2Norm操作 | 传入参数执行vectorL2Norm操作 |
| `vectorNorm` | vec: ArrayData, degree: float, funcName: UTF8String | `Float` | vectorNorm操作 | 传入参数执行vectorNorm操作 |
| `vectorNormalize` | vec: ArrayData, degree: float, funcName: UTF8String | `ArrayData` | 正常相关功能 | 传入参数执行正常相关功能 |
| `vectorNormalizeWithNorm` | vec: ArrayData, norm: float | `ArrayData` | 正常相关功能 | 传入参数执行正常相关功能 |

--------|------|----------|------|------|
| `visit` | dataPageV1: DataPageV1 | `Integer` | 访问相关功能 | 传入参数执行访问相关功能 |
| `visit` | dataPageV2: DataPageV2 | `Integer` | 访问相关功能 | 传入参数执行访问相关功能 |

--------|------|----------|------|------|
| `initFromPage` | valueCount: int, in: ByteBufferInputStream | `void` | 初始化FromPage相关功能 | 传入参数执行初始化FromPage相关功能 |
| `readByte` | 无 | `byte` | 读取Byte相关功能 | 调用该方法执行读取Byte相关功能 |
| `readBytes` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Bytes相关功能 | 传入参数执行读取Bytes相关功能 |
| `readInteger` | 无 | `int` | 读取Integer相关功能 | 调用该方法执行读取Integer相关功能 |
| `readIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Integers相关功能 | 传入参数执行读取Integers相关功能 |
| `readIntegersWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean | `void` | 读取IntegersWithRebase相关功能 | 传入参数执行读取IntegersWithRebase相关功能 |
| `readLong` | 无 | `long` | 读取Long相关功能 | 调用该方法执行读取Long相关功能 |
| `readLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Longs相关功能 | 传入参数执行读取Longs相关功能 |
| `readLongsWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean, timeZone: String | `void` | 读取LongsWithRebase相关功能 | 传入参数执行读取LongsWithRebase相关功能 |
| `readShort` | 无 | `short` | 读取Short相关功能 | 调用该方法执行读取Short相关功能 |
| `readShorts` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Shorts相关功能 | 传入参数执行读取Shorts相关功能 |
| `readUnsignedIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedIntegers相关功能 | 传入参数执行读取UnsignedIntegers相关功能 |
| `readUnsignedLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedLongs相关功能 | 传入参数执行读取UnsignedLongs相关功能 |
| `skipBytes` | total: int | `void` | 跳过Bytes相关功能 | 传入参数执行跳过Bytes相关功能 |
| `skipIntegers` | total: int | `void` | 跳过Integers相关功能 | 传入参数执行跳过Integers相关功能 |
| `skipLongs` | total: int | `void` | 跳过Longs相关功能 | 传入参数执行跳过Longs相关功能 |
| `skipShorts` | total: int | `void` | 跳过Shorts相关功能 | 传入参数执行跳过Shorts相关功能 |

--------|------|----------|------|------|
| `initFromPage` | valueCount: int, in: ByteBufferInputStream | `void` | 初始化FromPage相关功能 | 传入参数执行初始化FromPage相关功能 |
| `readBinary` | len: int | `Binary` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBinary` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readGeography` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geography相关功能 | 传入参数执行读取Geography相关功能 |
| `readGeometry` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geometry相关功能 | 传入参数执行读取Geometry相关功能 |
| `setPreviousReader` | reader: ValuesReader | `void` | 设置PreviousReader相关功能 | 传入参数执行设置PreviousReader相关功能 |
| `skipBinary` | total: int | `void` | 跳过Binary相关功能 | 传入参数执行跳过Binary相关功能 |

--------|------|----------|------|------|
| `getBytes` | rowId: int | `ByteBuffer` | 获取Bytes相关功能 | 传入参数执行获取Bytes相关功能 |
| `initFromPage` | valueCount: int, in: ByteBufferInputStream | `void` | 初始化FromPage相关功能 | 传入参数执行初始化FromPage相关功能 |
| `readBinary` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readGeography` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geography相关功能 | 传入参数执行读取Geography相关功能 |
| `readGeometry` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geometry相关功能 | 传入参数执行读取Geometry相关功能 |
| `skipBinary` | total: int | `void` | 跳过Binary相关功能 | 传入参数执行跳过Binary相关功能 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `enableReturningBatches` | 无 | `void` | 启用ReturningBatches相关功能 | 调用该方法执行启用ReturningBatches相关功能 |
| `getCurrentValue` | 无 | `Object` | 获取CurrentValue相关功能 | 调用该方法执行获取CurrentValue相关功能 |
| `getProgress` | 无 | `float` | 获取Progress相关功能 | 调用该方法执行获取Progress相关功能 |
| `initBatch` | partitionColumns: StructType, partitionValues: InternalRow | `void` | 初始化Batch相关功能 | 传入参数执行初始化Batch相关功能 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext, inputFile: Option<HadoopInputFile>, inputStream: Option<SeekableInputStream>, fileFooter: Option<ParquetMetadata> | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | path: String, columns: List<String> | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | fileSchema: MessageType, requestedSchema: MessageType, rowGroupReader: ParquetRowGroupReader, totalRowCount: int | `void` | 初始化插件 | 初始化目录插件 |
| `nextBatch` | 无 | `boolean` | 之后Batch相关功能 | 调用该方法执行之后Batch相关功能 |
| `nextKeyValue` | 无 | `boolean` | 之后KeyValue相关功能 | 调用该方法执行之后KeyValue相关功能 |
| `resultBatch` | 无 | `ColumnarBatch` | resultBatch操作 | 调用该方法执行resultBatch操作 |

--------|------|----------|------|------|
| `initFromPage` | valueCount: int, in: ByteBufferInputStream | `void` | 初始化FromPage相关功能 | 传入参数执行初始化FromPage相关功能 |
| `readBinary` | total: int, v: WritableColumnVector, rowId: int | `void` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBinary` | len: int | `Binary` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBoolean` | 无 | `boolean` | 读取Boolean相关功能 | 调用该方法执行读取Boolean相关功能 |
| `readBooleans` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Booleans相关功能 | 传入参数执行读取Booleans相关功能 |
| `readByte` | 无 | `byte` | 读取Byte相关功能 | 调用该方法执行读取Byte相关功能 |
| `readBytes` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Bytes相关功能 | 传入参数执行读取Bytes相关功能 |
| `readDouble` | 无 | `double` | 读取Double相关功能 | 调用该方法执行读取Double相关功能 |
| `readDoubles` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Doubles相关功能 | 传入参数执行读取Doubles相关功能 |
| `readFloat` | 无 | `float` | 读取Float相关功能 | 调用该方法执行读取Float相关功能 |
| `readFloats` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Floats相关功能 | 传入参数执行读取Floats相关功能 |
| `readGeography` | total: int, v: WritableColumnVector, rowId: int | `void` | 读取Geography相关功能 | 传入参数执行读取Geography相关功能 |
| `readGeometry` | total: int, v: WritableColumnVector, rowId: int | `void` | 读取Geometry相关功能 | 传入参数执行读取Geometry相关功能 |
| `readInteger` | 无 | `int` | 读取Integer相关功能 | 调用该方法执行读取Integer相关功能 |
| `readIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Integers相关功能 | 传入参数执行读取Integers相关功能 |
| `readIntegersWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean | `void` | 读取IntegersWithRebase相关功能 | 传入参数执行读取IntegersWithRebase相关功能 |
| `readLong` | 无 | `long` | 读取Long相关功能 | 调用该方法执行读取Long相关功能 |
| `readLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Longs相关功能 | 传入参数执行读取Longs相关功能 |
| `readLongsWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean, timeZone: String | `void` | 读取LongsWithRebase相关功能 | 传入参数执行读取LongsWithRebase相关功能 |
| `readShort` | 无 | `short` | 读取Short相关功能 | 调用该方法执行读取Short相关功能 |
| `readShorts` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Shorts相关功能 | 传入参数执行读取Shorts相关功能 |
| `readUnsignedIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedIntegers相关功能 | 传入参数执行读取UnsignedIntegers相关功能 |
| `readUnsignedLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedLongs相关功能 | 传入参数执行读取UnsignedLongs相关功能 |
| `skip` | 无 | `void` | 跳过相关功能 | 调用该方法执行跳过相关功能 |
| `skipBinary` | total: int | `void` | 跳过Binary相关功能 | 传入参数执行跳过Binary相关功能 |
| `skipBooleans` | total: int | `void` | 跳过Booleans相关功能 | 传入参数执行跳过Booleans相关功能 |
| `skipBytes` | total: int | `void` | 跳过Bytes相关功能 | 传入参数执行跳过Bytes相关功能 |
| `skipDoubles` | total: int | `void` | 跳过Doubles相关功能 | 传入参数执行跳过Doubles相关功能 |
| `skipFixedLenByteArray` | total: int, len: int | `void` | 跳过FixedLenByteArray相关功能 | 传入参数执行跳过FixedLenByteArray相关功能 |
| `skipFloats` | total: int | `void` | 跳过Floats相关功能 | 传入参数执行跳过Floats相关功能 |
| `skipIntegers` | total: int | `void` | 跳过Integers相关功能 | 传入参数执行跳过Integers相关功能 |
| `skipLongs` | total: int | `void` | 跳过Longs相关功能 | 传入参数执行跳过Longs相关功能 |
| `skipShorts` | total: int | `void` | 跳过Shorts相关功能 | 传入参数执行跳过Shorts相关功能 |

--------|------|----------|------|------|
| `readBinary` | len: int | `Binary` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBinary` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBooleans` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Booleans相关功能 | 传入参数执行读取Booleans相关功能 |
| `readByte` | 无 | `byte` | 读取Byte相关功能 | 调用该方法执行读取Byte相关功能 |
| `readBytes` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Bytes相关功能 | 传入参数执行读取Bytes相关功能 |
| `readDoubles` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Doubles相关功能 | 传入参数执行读取Doubles相关功能 |
| `readFloats` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Floats相关功能 | 传入参数执行读取Floats相关功能 |
| `readGeography` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geography相关功能 | 传入参数执行读取Geography相关功能 |
| `readGeometry` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geometry相关功能 | 传入参数执行读取Geometry相关功能 |
| `readIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Integers相关功能 | 传入参数执行读取Integers相关功能 |
| `readIntegersWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean | `void` | 读取IntegersWithRebase相关功能 | 传入参数执行读取IntegersWithRebase相关功能 |
| `readLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Longs相关功能 | 传入参数执行读取Longs相关功能 |
| `readLongsWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean, timeZone: String | `void` | 读取LongsWithRebase相关功能 | 传入参数执行读取LongsWithRebase相关功能 |
| `readShort` | 无 | `short` | 读取Short相关功能 | 调用该方法执行读取Short相关功能 |
| `readShorts` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Shorts相关功能 | 传入参数执行读取Shorts相关功能 |
| `readUnsignedIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedIntegers相关功能 | 传入参数执行读取UnsignedIntegers相关功能 |
| `readUnsignedLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedLongs相关功能 | 传入参数执行读取UnsignedLongs相关功能 |
| `skip` | 无 | `void` | 跳过相关功能 | 调用该方法执行跳过相关功能 |
| `skipBinary` | total: int | `void` | 跳过Binary相关功能 | 传入参数执行跳过Binary相关功能 |
| `skipBooleans` | total: int | `void` | 跳过Booleans相关功能 | 传入参数执行跳过Booleans相关功能 |
| `skipBytes` | total: int | `void` | 跳过Bytes相关功能 | 传入参数执行跳过Bytes相关功能 |
| `skipDoubles` | total: int | `void` | 跳过Doubles相关功能 | 传入参数执行跳过Doubles相关功能 |
| `skipFixedLenByteArray` | total: int, len: int | `void` | 跳过FixedLenByteArray相关功能 | 传入参数执行跳过FixedLenByteArray相关功能 |
| `skipFloats` | total: int | `void` | 跳过Floats相关功能 | 传入参数执行跳过Floats相关功能 |
| `skipIntegers` | total: int | `void` | 跳过Integers相关功能 | 传入参数执行跳过Integers相关功能 |
| `skipLongs` | total: int | `void` | 跳过Longs相关功能 | 传入参数执行跳过Longs相关功能 |
| `skipShorts` | total: int | `void` | 跳过Shorts相关功能 | 传入参数执行跳过Shorts相关功能 |


### ViewChange
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `property` | 无 | `String` | property操作 | 调用该方法执行property操作 |
| `value` | 无 | `String` | 获取度量指标值 | 返回度量指标数值 |


### ViewInfo
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `currentCatalog` | 无 | `String` | 当前Catalog相关功能 | 调用该方法执行当前Catalog相关功能 |
| `ident` | 无 | `Identifier` | ident操作 | 调用该方法执行ident操作 |
| `properties` | 无 | `Map&lt;String, String&gt;` | properties操作 | 调用该方法执行properties操作 |
| `schema` | 无 | `StructType` | 获取schema | 调用该方法执行schema操作 |
| `sql` | 无 | `String` | 执行SQL查询 | 调用该方法执行sql操作 |

--------|------|----------|------|------|
| `getParseError` | 无 | `String` | 获取ParseError相关功能 | 调用该方法执行获取ParseError相关功能 |
| `getPosition` | 无 | `long` | 获取Position相关功能 | 调用该方法执行获取Position相关功能 |

--------|------|----------|------|------|
| `read` | wkb: byte&lt;&gt; | `GeometryModel` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |
| `read` | wkb: byte&lt;&gt;, srid: int | `GeometryModel` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |

--------|------|----------|------|------|
| `addElementsAppended` | num: int | `void` | 增加已追加元素计数，用于手动调整追加位置 | `vector.addElementsAppended(5);
// 增加5个元素的计数` |
| `appendArray` | length: int | `int` | 追加数组数据，返回追加的起始位置 | `int startPos = vector.appendArray(3);
// 追加长度为3的数组` |
| `appendBoolean` | v: boolean | `int` | 追加单个布尔值，返回追加位置 | `int pos = vector.appendBoolean(true);` |
| `appendBooleans` | count: int, v: boolean | `int` | 批量追加相同布尔值，返回起始位置 | `int startPos = vector.appendBooleans(10, true);
// 追加10个true值` |
| `appendBooleans` | count: int, src: byte, offset: int | `int` | 从字节位图追加布尔值，src每bit对应一个布尔 | `byte bitmap = 0x0F;  // 低4位为true
int pos = vector.appendBooleans(4, bitmap, 0);` |
| `appendByte` | v: byte | `int` | 追加单个字节值 | `int pos = vector.appendByte((byte) 100);` |
| `appendByteArray` | value: byte[], offset: int, length: int | `int` | 追加字节数组的部分内容 | `byte[] data = {1, 2, 3, 4, 5};
int pos = vector.appendByteArray(data, 1, 3);  // 追加{2,3,4}` |
| `appendBytes` | count: int, v: byte | `int` | 批量追加相同字节值 | `int pos = vector.appendBytes(100, (byte) 0);
// 追加100个0字节` |
| `appendBytes` | length: int, src: byte[], offset: int | `int` | 批量追加字节数组内容 | `byte[] src = {1, 2, 3};
int pos = vector.appendBytes(3, src, 0);` |
| `appendDouble` | v: double | `int` | 追加单个双精度值 | `int pos = vector.appendDouble(3.14);` |
| `appendDoubles` | count: int, v: double | `int` | 批量追加相同双精度值 | `int pos = vector.appendDoubles(10, 1.5);` |
| `appendDoubles` | length: int, src: double[], offset: int | `int` | 批量追加双精度数组内容 | `double[] values = {1.1, 2.2, 3.3};
int pos = vector.appendDoubles(3, values, 0);` |
| `appendFloat` | v: float | `int` | 追加单个单精度值 | `int pos = vector.appendFloat(2.5f);` |
| `appendFloats` | count: int, v: float | `int` | 批量追加相同单精度值 | `int pos = vector.appendFloats(5, 1.0f);` |
| `appendFloats` | length: int, src: float[], offset: int | `int` | 批量追加单精度数组内容 | `float[] values = {1.0f, 2.0f};
int pos = vector.appendFloats(2, values, 0);` |
| `appendInt` | v: int | `int` | 追加单个整数值 | `int pos = vector.appendInt(42);` |
| `appendInts` | count: int, v: int | `int` | 批量追加相同整数值 | `int pos = vector.appendInts(100, 0);` |
| `appendInts` | length: int, src: int[], offset: int | `int` | 批量追加整数数组内容 | `int[] values = {1, 2, 3, 4, 5};
int pos = vector.appendInts(3, values, 2);  // 追加{3,4,5}` |
| `appendLong` | v: long | `int` | 追加单个长整数值 | `int pos = vector.appendLong(100000L);` |
| `appendLongs` | count: int, v: long | `int` | 批量追加相同长整数值 | `int pos = vector.appendLongs(10, 0L);` |
| `appendLongs` | length: int, src: long[], offset: int | `int` | 批量追加长整数数组内容 | `long[] values = {1L, 2L, 3L};
int pos = vector.appendLongs(3, values, 0);` |
| `appendNotNull` | 无 | `int` | 追加非null标记，返回追加位置 | `int pos = vector.appendNotNull();` |
| `appendNotNulls` | count: int | `int` | 批量追加非null标记 | `int pos = vector.appendNotNulls(100);` |
| `appendNull` | 无 | `int` | 追加null标记，返回追加位置 | `int pos = vector.appendNull();` |
| `appendNulls` | count: int | `int` | 批量追加null标记 | `int pos = vector.appendNulls(10);` |
| `appendObjects` | length: int, value: Object | `Optional<Integer>` | 追加对象数组（不常用，部分类型不支持） | `Optional&lt;Integer&gt; pos = vector.appendObjects(1, obj);` |
| `appendShort` | v: short | `int` | 追加单个短整数值 | `int pos = vector.appendShort((short) 100);` |
| `appendShorts` | count: int, v: short | `int` | 批量追加相同短整数值 | `int pos = vector.appendShorts(5, (short) 10);` |
| `appendShorts` | length: int, src: short[], offset: int | `int` | 批量追加短整数数组内容 | `short[] values = {1, 2, 3};
int pos = vector.appendShorts(3, values, 0);` |
| `appendStruct` | isNull: boolean | `int` | 追加Struct结构，isNull指定是否为null | `int pos = vector.appendStruct(false);
// 需后续填充子字段` |
| `arrayData` | 无 | `WritableColumnVector` | 获取存储数组数据的底层列向量 | `WritableColumnVector arrData = vector.arrayData();
// 用于写入Array类型的元素` |
| `close` | 无 | `void` | 关闭列向量，释放内存和子列向量 | `vector.close();` |
| `closeIfFreeable` | 无 | `void` | 无操作（实现类可能重写） | `// 默认为空实现` |
| `getArray` | rowId: int | `ColumnarArray` | 获取指定行的数组数据 | `ColumnarArray arr = vector.getArray(0);` |
| `getChild` | ordinal: int | `WritableColumnVector` | 获取嵌套类型的子列向量 | `WritableColumnVector child = vector.getChild(0);
// 用于写入Struct字段` |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取指定行的Decimal值 | `Decimal dec = vector.getDecimal(0, 10, 2);` |
| `getDictionaryIds` | 无 | `WritableColumnVector` | 获取字典编码的ID列向量 | `WritableColumnVector dictIds = vector.getDictionaryIds();` |
| `getElementsAppended` | 无 | `int` | 获取已追加元素的数量 | `int count = vector.getElementsAppended();` |
| `getMap` | rowId: int | `ColumnarMap` | 获取指定行的Map数据 | `ColumnarMap map = vector.getMap(0);` |
| `getNumChildren` | 无 | `int` | 获取子列向量数量 | `int numChildren = vector.getNumChildren();` |
| `getUTF8String` | rowId: int | `UTF8String` | 获取指定行的UTF8字符串 | `UTF8String str = vector.getUTF8String(0);` |
| `hasDictionary` | 无 | `boolean` | 检查是否使用字典编码 | `if (vector.hasDictionary()) {
    // 使用字典解码读取
}` |
| `hasNull` | 无 | `boolean` | 检查是否存在null值 | `boolean hasNulls = vector.hasNull();` |
| `isAllNull` | 无 | `boolean` | 检查是否所有值都是null | `boolean allNull = vector.isAllNull();` |
| `isMissing` | 无 | `boolean` | 检查是否为缺失状态 | `boolean missing = vector.isMissing();` |
| `numNulls` | 无 | `int` | 返回null值数量 | `int nullCount = vector.numNulls();` |
| `putBooleans` | rowId: int, count: int, src: byte, srcIndex: int | `void` | 从位图写入布尔值到指定位置 | `byte bitmap = 0x55;
vector.putBooleans(0, 4, bitmap, 0);` |
| `putByteArray` | rowId: int, value: byte[] | `int` | 写入字节数组到指定行 | `int offset = vector.putByteArray(0, new byte[]{1,2,3});` |
| `putByteArray` | rowId: int, src: ByteBuffer, srcPosition: int, length: int | `int` | 从ByteBuffer写入字节数组 | `ByteBuffer buf = ByteBuffer.wrap(data);
int offset = vector.putByteArray(0, buf, 0, 10);` |
| `putDecimal` | rowId: int, value: Decimal, precision: int | `void` | 写入Decimal值到指定行 | `Decimal dec = Decimal.apply(123.45);
vector.putDecimal(0, dec, 10);` |
| `putInterval` | rowId: int, value: CalendarInterval | `void` | 写入时间间隔到指定行 | `CalendarInterval interval = new CalendarInterval(1, 2, 1000L);
vector.putInterval(0, interval);` |
| `reserve` | requiredCapacity: int | `void` | 预留指定容量的内存空间，写入前必须调用 | `vector.reserve(1000);
// 预留1000个元素的容量` |
| `reserveAdditional` | additionalCapacity: int | `void` | 预留额外的内存空间（追加当前容量） | `vector.reserveAdditional(100);
// 增加100容量` |
| `reserveDictionaryIds` | capacity: int | `WritableColumnVector` | 为字典编码预留ID存储空间 | `WritableColumnVector dictIds = vector.reserveDictionaryIds(1000);` |
| `reset` | 无 | `void` | 重置列向量，清空数据准备重新写入 | `vector.reset();
// 清空数据，重置计数器` |
| `setDictionary` | dictionary: Dictionary | `void` | 设置字典编码对象 | `vector.setDictionary(dictionary);
// 启用字典解码` |
| `setIsConstant` | 无 | `void` | 设置为常量列向量 | `vector.setIsConstant();
// 标记为常量值列` |
| `setMissing` | 无 | `void` | 设置为缺失状态 | `vector.setMissing();` |


### WriteBuilder
**包路径**: `org.apache.spark.sql.connector.write`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toBatch` | 无 | `BatchWrite` | 将行迭代器转换为列式批处理 | 将行迭代器转为列式批处理 |
| `toStreaming` | 无 | `StreamingWrite` | toStreaming操作 | 调用该方法执行toStreaming操作 |

---

## Streaming流处理


### GetPrimaryKeysOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |


### TimerWithCustomTimevoid
**包路径**: `org.apache.spark.network.util`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `dump` | outputStream: OutputStream | `void` | dump操作 | 传入参数执行dump操作 |
| `getMax` | 无 | `long` | 获取Max相关功能 | 调用该方法执行获取Max相关功能 |
| `getMean` | 无 | `double` | 获取Mean相关功能 | 调用该方法执行获取Mean相关功能 |
| `getMin` | 无 | `long` | 获取Min相关功能 | 调用该方法执行获取Min相关功能 |
| `getSnapshot` | 无 | `Snapshot` | 获取Snapshot相关功能 | 调用该方法执行获取Snapshot相关功能 |
| `getStdDev` | 无 | `double` | 获取StdDev相关功能 | 调用该方法执行获取StdDev相关功能 |
| `getValue` | v: double | `double` | 获取列的默认值 | 返回列默认值的Literal对象 |
| `size` | 无 | `int` | 计算大小 | 调用该方法执行size操作 |


