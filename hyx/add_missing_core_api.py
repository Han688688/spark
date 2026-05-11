#!/usr/bin/env python3
"""
补充缺失的核心Spark Java API
"""

# 需要补充的核心API
MISSING_CORE_API = '''
---

## SparkConf（配置）

### SparkConf
**包路径**: `org.apache.spark`
**说明**: Spark配置类，用于设置各种Spark参数。
**方法数量**: 15+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `set` | String key, String value | `SparkConf` | 设置配置项 | `SparkConf conf = new SparkConf().set("spark.executor.memory", "4g");` |
| `setMaster` | String master | `SparkConf` | 设置运行模式 | `conf.setMaster("local[4]");` |
| `setAppName` | String name | `SparkConf` | 设置应用名称 | `conf.setAppName("My Spark App");` |
| `setSparkHome` | String home | `SparkConf` | 设置Spark安装目录 | `conf.setSparkHome("/opt/spark");` |
| `setExecutorEnv` | String key, String value | `SparkConf` | 设置Executor环境变量 | `conf.setExecutorEnv("JAVA_HOME", "/usr/lib/jvm/java-11");` |
| `setJars` | String... jars | `SparkConf` | 设置依赖JAR包 | `conf.setJars("hdfs://libs/my-lib.jar");` |
| `setAll` | Map[String, String] settings | `SparkConf` | 批量设置配置 | `Map<String, String> settings = new HashMap<>();<br>settings.put("spark.executor.cores", "2");<br>conf.setAll(settings);` |
| `get` | String key | `String` | 获取配置值 | `String value = conf.get("spark.executor.memory");` |
| `get` | String key, String defaultValue | `String` | 获取配置值，带默认值 | `String value = conf.get("spark.executor.memory", "2g");` |
| `getAll` | 无 | `Array[Tuple2[String, String]]` | 获取所有配置 | `Tuple2<String, String>[] all = conf.getAll();` |
| `contains` | String key | `Boolean` | 检查配置是否存在 | `boolean exists = conf.contains("spark.executor.memory");` |
| `remove` | String key | `SparkConf` | 移除配置项 | `conf.remove("spark.executor.memory");` |
| `clone` | 无 | `SparkConf` | 克隆配置 | `SparkConf cloned = conf.clone();` |

---

## Broadcast & Accumulator（共享变量）

### Broadcast[T]
**包路径**: `org.apache.spark.broadcast`
**说明**: 广播变量，将数据高效分发到所有Executor。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `value` | 无 | `T` | 获取广播变量的值 | `Broadcast<Map<String, String>> config = sc.broadcast(configMap);<br>Map<String, String> map = config.value();` |
| `unpersist` | 无 | `Unit` | 从Executor释放广播变量 | `config.unpersist();` |
| `unpersist` | Boolean blocking | `Unit` | 从Executor释放，指定阻塞 | `config.unpersist(true);  // 阻塞等待释放` |
| `destroy` | 无 | `Unit` | 完全销毁广播变量 | `config.destroy();  // Driver和Executor都释放` |

### Accumulator[T]
**包路径**: `org.apache.spark`
**说明**: 累加器，用于聚合Worker端数据到Driver。仅支持累加操作。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | T term | `Unit` | 累加值（只能在Worker端调用） | `Accumulator<Integer> acc = sc.accumulator(0);<br>rdd.foreach(x -> acc.add(x));` |
| `value` | 无 | `T` | 获取累加结果（只能在Driver端调用） | `int total = acc.value();` |
| `setValue` | T newValue | `Unit` | 设置值（只能在Driver端调用） | `acc.setValue(100);` |
| `isZero` | 无 | `Boolean` | 检查是否为零值 | `boolean zero = acc.isZero();` |
| `reset` | 无 | `Unit` | 重置为零值 | `acc.reset();` |
| `name` | 无 | `String` | 获取累加器名称 | `String name = acc.name();` |

### LongAccumulator / DoubleAccumulator / CollectionAccumulator
**包路径**: `org.apache.spark.util`
**说明**: 特化累加器，支持特定类型的累加。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | Long/Double/T term | `Unit` | 累加值 | `LongAccumulator longAcc = sc.sc().longAccumulator("counter");<br>longAcc.add(10);` |
| `value` | 无 | `Long/Double/List[T]` | 获取累加结果 | `long sum = longAcc.value();` |
| `count` | 无 | `Long` | 获取计数（LongAccumulator） | `long count = longAcc.count();` |
| `avg` | 无 | `Double` | 获取平均值（LongAccumulator/DoubleAccumulator） | `double avg = longAcc.avg();` |
| `sum` | 无 | `Long/Double` | 获取总和 | `long sum = longAcc.sum();` |

---

## SQL辅助类

### Column
**包路径**: `org.apache.spark.sql`
**说明**: DataFrame列引用，用于构建SQL表达式。
**方法数量**: 40+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `col` | String colName | `Column` | 创建列引用（静态方法） | `Column c = col("age");` |
| `equalTo` | Object other | `Column` | 等于条件 | `df.filter(col("id").equalTo(100));` |
| `notEqual` | Object other | `Column` | 不等于条件 | `df.filter(col("status").notEqual("deleted"));` |
| `gt` | Object other | `Column` | 大于条件 | `df.filter(col("age").gt(18));` |
| `lt` | Object other | `Column` | 小于条件 | `df.filter(col("price").lt(1000));` |
| `geq` | Object other | `Column` | 大于等于条件 | `df.filter(col("score").geq(60));` |
| `leq` | Object other | `Column` | 小于等于条件 | `df.filter(col("qty").leq(10));` |
| `isNull` | 无 | `Column` | 判断是否为null | `df.filter(col("email").isNull());` |
| `isNotNull` | 无 | `Column` | 判断是否非null | `df.filter(col("email").isNotNull());` |
| `and` | Column other | `Column` | 逻辑与 | `df.filter(col("age").gt(18).and(col("status").equalTo("active")));` |
| `or` | Column other | `Column` | 逻辑或 | `df.filter(col("type").equalTo("A").or(col("type").equalTo("B")));` |
| `plus` | Object other | `Column` | 加法 | `df.withColumn("total", col("price").plus(col("tax")));` |
| `minus` | Object other | `Column` | 减法 | `df.withColumn("diff", col("end").minus(col("start")));` |
| `multiply` | Object other | `Column` | 乘法 | `df.withColumn("double", col("value").multiply(2));` |
| `divide` | Object other | `Column` | 除法 | `df.withColumn("avg", col("total").divide(col("count")));` |
| `mod` | Object other | `Column` | 取模 | `df.filter(col("id").mod(2).equalTo(0));  // 奇数` |
| `like` | String literal | `Column` | LIKE匹配 | `df.filter(col("name").like("%John%"));` |
| `rlike` | String regex | `Column` | 正则匹配 | `df.filter(col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$"));` |
| `contains` | String other | `Column` | 包含字符串 | `df.filter(col("content").contains("error"));` |
| `startsWith` | String prefix | `Column` | 以...开始 | `df.filter(col("name").startsWith("John"));` |
| `endsWith` | String suffix | `Column` | 以...结束 | `df.filter(col("filename").endsWith(".csv"));` |
| `alias` | String alias | `Column` | 设置别名 | `df.select(col("id").alias("user_id"));` |
| `as` | String alias | `Column` | 设置别名（同alias） | `df.select(col("id").as("user_id"));` |
| `cast` | DataType to | `Column` | 类型转换 | `df.withColumn("id_str", col("id").cast(DataTypes.StringType));` |
| `asc` | 无 | `Column` | 升序排序 | `df.orderBy(col("id").asc());` |
| `desc` | 无 | `Column` | 降序排序 | `df.orderBy(col("id").desc());` |
| `asc_nulls_first` | 无 | `Column` | 升序，null排前 | `df.orderBy(col("value").asc_nulls_first());` |
| `asc_nulls_last` | 无 | `Column` | 升序，null排后 | `df.orderBy(col("value").asc_nulls_last());` |
| `desc_nulls_first` | 无 | `Column` | 降序，null排前 | `df.orderBy(col("value").desc_nulls_first());` |
| `desc_nulls_last` | 无 | `Column` | 降序，null排后 | `df.orderBy(col("value").desc_nulls_last());` |
| `between` | Object lowerBound, Object upperBound | `Column` | 范围条件 | `df.filter(col("age").between(18, 65));` |
| `when` | Column condition, Object value | `Column` | CASE WHEN条件 | `df.withColumn("category", when(col("age").lt(18), "child")<br>    .when(col("age").lt(60), "adult")<br>    .otherwise("senior"));` |
| `otherwise` | Object value | `Column` | CASE WHEN默认值 | `when(col("score").geq(90), "A").otherwise("B");` |
| `over` | Window window | `Column` | 窗口函数 | `col("value").sum().over(Window.partitionBy("group"));` |
| `isNull` | 无 | `Column` | 判断null | `df.filter(col("name").isNull());` |
| `isNotNull` | 无 | `Column` | 判断非null | `df.filter(col("name").isNotNull());` |
| `isin` | Object... values | `Column` | IN条件 | `df.filter(col("status").isin("active", "pending", "running"));` |
| `in` | Column list | `Column` | IN子查询 | `df.filter(col("id").in(otherDf.select(col("user_id"))));` |
| `substr` | int startPos, int len | `Column` | 截取子串 | `df.withColumn("first3", col("name").substr(0, 3));` |
| `upper` | 无 | `Column` | 转大写 | `df.withColumn("upper_name", col("name").upper());` |
| `lower` | 无 | `Column` | 转小写 | `df.withColumn("lower_name", col("name").lower());` |

### functions（内置函数）
**包路径**: `org.apache.spark.sql.functions`
**说明**: Spark SQL内置函数集合，提供聚合、字符串、数学、日期等函数。
**方法数量**: 100+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `count` | Column e | `Column` | 计数 | `df.agg(count(col("id")));` |
| `countDistinct` | Column e, Column... es | `Column` | 唯一值计数 | `df.agg(countDistinct(col("user_id")));` |
| `sum` | Column e | `Column` | 求和 | `df.agg(sum(col("amount")));` |
| `sumDistinct` | Column e | `Column` | 唯一值求和 | `df.agg(sumDistinct(col("price")));` |
| `avg` | Column e | `Column` | 平均值 | `df.agg(avg(col("score")));` |
| `mean` | Column e | `Column` | 平均值（同avg） | `df.agg(mean(col("score")));` |
| `max` | Column e | `Column` | 最大值 | `df.agg(max(col("price")));` |
| `min` | Column e | `Column` | 最小值 | `df.agg(min(col("price")));` |
| `first` | Column e | `Column` | 第一个值 | `df.agg(first(col("name")));` |
| `first` | Column e, boolean ignoreNulls | `Column` | 第一个非null值 | `df.agg(first(col("name"), true));` |
| `last` | Column e | `Column` | 最后一个值 | `df.agg(last(col("name")));` |
| `last` | Column e, boolean ignoreNulls | `Column` | 最后一个非null值 | `df.agg(last(col("name"), true));` |
| `collect_list` | Column e | `Column` | 收集为数组（保留重复） | `df.groupBy("group").agg(collect_list(col("value")));` |
| `collect_set` | Column e | `Column` | 收集为数组（去重） | `df.groupBy("group").agg(collect_set(col("value")));` |
| `approx_count_distinct` | Column e | `Column` | 近似唯一值计数 | `df.agg(approx_count_distinct(col("user_id")));` |
| `approx_count_distinct` | Column e, double rsd | `Column` | 近似计数，指定误差率 | `df.agg(approx_count_distinct(col("user_id"), 0.05));` |
| `variance` | Column e | `Column` | 方差 | `df.agg(variance(col("value")));` |
| `var_samp` | Column e | `Column` | 样本方差 | `df.agg(var_samp(col("value")));` |
| `var_pop` | Column e | `Column` | 总体方差 | `df.agg(var_pop(col("value")));` |
| `stddev` | Column e | `Column` | 标准差 | `df.agg(stddev(col("value")));` |
| `stddev_samp` | Column e | `Column` | 样本标准差 | `df.agg(stddev_samp(col("value")));` |
| `stddev_pop` | Column e | `Column` | 总体标准差 | `df.agg(stddev_pop(col("value")));` |
| `skewness` | Column e | `Column` | 偏度 | `df.agg(skewness(col("value")));` |
| `kurtosis` | Column e | `Column` | 峰度 | `df.agg(kurtosis(col("value")));` |
| `corr` | Column col1, Column col2 | `Column` | Pearson相关系数 | `df.agg(corr(col("price"), col("rating")));` |
| `covar_pop` | Column col1, Column col2 | `Column` | 总体协方差 | `df.agg(covar_pop(col("x"), col("y")));` |
| `covar_samp` | Column col1, Column col2 | `Column` | 样本协方差 | `df.agg(covar_samp(col("x"), col("y")));` |
| `lit` | Object literal | `Column` | 创建常量列 | `df.withColumn("constant", lit(100));` |
| `col` | String colName | `Column` | 创建列引用 | `df.select(col("name"));` |
| `column` | String colName | `Column` | 创建列引用（同col） | `df.select(column("name"));` |
| `when` | Column condition, Object value | `Column` | CASE WHEN | `when(col("age").lt(18), "child").otherwise("adult");` |
| `concat` | Column... exprs | `Column` | 连接字符串 | `df.withColumn("full_name", concat(col("first"), lit(" "), col("last")));` |
| `concat_ws` | String sep, Column... exprs | `Column` | 用分隔符连接字符串 | `df.withColumn("tags", concat_ws(",", col("tag1"), col("tag2")));` |
| `split` | Column str, String pattern | `Column` | 分割字符串为数组 | `df.withColumn("words", split(col("sentence"), " "));` |
| `substring` | Column str, int pos, int len | `Column` | 截取子串 | `df.withColumn("abbr", substring(col("name"), 0, 3));` |
| `length` | Column e | `Column` | 字符串长度 | `df.withColumn("name_len", length(col("name")));` |
| `trim` | Column e | `Column` | 去除两端空白 | `df.withColumn("clean_name", trim(col("name")));` |
| `ltrim` | Column e | `Column` | 去除左侧空白 | `df.withColumn("clean", ltrim(col("name")));` |
| `rtrim` | Column e | `Column` | 去除右侧空白 | `df.withColumn("clean", rtrim(col("name")));` |
| `upper` | Column e | `Column` | 转大写 | `df.withColumn("upper_name", upper(col("name")));` |
| `lower` | Column e | `Column` | 转小写 | `df.withColumn("lower_name", lower(col("name")));` |
| `initcap` | Column e | `Column` | 首字母大写 | `df.withColumn("capitalized", initcap(col("name")));` |
| `regexp_replace` | Column e, String pattern, String replacement | `Column` | 正则替换 | `df.withColumn("clean", regexp_replace(col("text"), "[0-9]+", ""));` |
| `regexp_extract` | Column e, String pattern, int idx | `Column` | 正则提取 | `df.withColumn("domain", regexp_extract(col("url"), "https?://([^/]+)", 1));` |
| `instr` | Column str, String substring | `Column` | 查找子串位置 | `df.filter(instr(col("name"), "John") > 0);` |
| `locate` | String substr, Column str | `Column` | 查找子串位置 | `df.filter(locate("John", col("name")) > 0);` |
| `replace` | Column src, Column search, Column replace | `Column` | 字符替换 | `df.withColumn("clean", replace(col("text"), lit("old"), lit("new")));` |
| `abs` | Column e | `Column` | 绝对值 | `df.withColumn("abs_value", abs(col("value")));` |
| `ceil` | Column e | `Column` | 向上取整 | `df.withColumn("rounded", ceil(col("value")));` |
| `floor` | Column e | `Column` | 向下取整 | `df.withColumn("rounded", floor(col("value")));` |
| `round` | Column e | `Column` | 四舍五入 | `df.withColumn("rounded", round(col("value")));` |
| `round` | Column e, int scale | `Column` | 四舍五入到指定小数位 | `df.withColumn("rounded", round(col("value"), 2));` |
| `bround` | Column e | `Column` | 银行家舍入 | `df.withColumn("rounded", bround(col("value")));` |
| `exp` | Column e | `Column` | e指数 | `df.withColumn("exp_val", exp(col("log_value")));` |
| `log` | Column e | `Column` | 自然对数 | `df.withColumn("log_val", log(col("value")));` |
| `log10` | Column e | `Column` | 10为底对数 | `df.withColumn("log10_val", log10(col("value")));` |
| `log2` | Column e | `Column` | 2为底对数 | `df.withColumn("log2_val", log2(col("value")));` |
| `pow` | Column l, Column r | `Column` | 幂运算 | `df.withColumn("squared", pow(col("value"), lit(2)));` |
| `sqrt` | Column e | `Column` | 平方根 | `df.withColumn("sqrt_val", sqrt(col("value")));` |
| `sin` | Column e | `Column` | 正弦 | `df.withColumn("sin_val", sin(col("angle")));` |
| `cos` | Column e | `Column` | 余弦 | `df.withColumn("cos_val", cos(col("angle")));` |
| `tan` | Column e | `Column` | 正切 | `df.withColumn("tan_val", tan(col("angle")));` |
| `asin` | Column e | `Column` | 反正弦 | `df.withColumn("asin_val", asin(col("value")));` |
| `acos` | Column e | `Column` | 反余弦 | `df.withColumn("acos_val", acos(col("value")));` |
| `atan` | Column e | `Column` | 反正切 | `df.withColumn("atan_val", atan(col("value")));` |
| `rand` | 无 | `Column` | 随机数（0-1） | `df.withColumn("random", rand());` |
| `randn` | 无 | `Column` | 正态分布随机数 | `df.withColumn("normal", randn());` |
| `current_date` | 无 | `Column` | 当前日期 | `df.withColumn("today", current_date());` |
| `current_timestamp` | 无 | `Column` | 当前时间戳 | `df.withColumn("now", current_timestamp());` |
| `date_add` | Column start, int days | `Column` | 日期加天数 | `df.withColumn("future", date_add(col("date"), 30));` |
| `date_sub` | Column start, int days | `Column` | 日期减天数 | `df.withColumn("past", date_sub(col("date"), 30));` |
| `datediff` | Column end, Column start | `Column` | 日期差（天数） | `df.withColumn("days_diff", datediff(col("end_date"), col("start_date")));` |
| `add_months` | Column startDate, int numMonths | `Column` | 加月份 | `df.withColumn("future", add_months(col("date"), 12));` |
| `months_between` | Column end, Column start | `Column` | 月份差 | `df.withColumn("months", months_between(col("end_date"), col("start_date")));` |
| `year` | Column e | `Column` | 提取年份 | `df.withColumn("year", year(col("date")));` |
| `month` | Column e | `Column` | 提取月份 | `df.withColumn("month", month(col("date")));` |
| `dayofmonth` | Column e | `Column` | 提取日 | `df.withColumn("day", dayofmonth(col("date")));` |
| `dayofweek` | Column e | `Column` | 提取星期几（1=周日） | `df.withColumn("weekday", dayofweek(col("date")));` |
| `dayofyear` | Column e | `Column` | 提取年中第几天 | `df.withColumn("daynum", dayofyear(col("date")));` |
| `weekofyear` | Column e | `Column` | 提取年中第几周 | `df.withColumn("week", weekofyear(col("date")));` |
| `hour` | Column e | `Column` | 提取小时 | `df.withColumn("hour", hour(col("timestamp")));` |
| `minute` | Column e | `Column` | 提取分钟 | `df.withColumn("minute", minute(col("timestamp")));` |
| `second` | Column e | `Column` | 提取秒 | `df.withColumn("second", second(col("timestamp")));` |
| `to_date` | Column e | `Column` | 转为日期 | `df.withColumn("date", to_date(col("date_str")));` |
| `to_date` | Column e, String fmt | `Column` | 指定格式转日期 | `df.withColumn("date", to_date(col("date_str"), "yyyy-MM-dd"));` |
| `to_timestamp` | Column e | `Column` | 转为时间戳 | `df.withColumn("ts", to_timestamp(col("ts_str")));` |
| `to_timestamp` | Column e, String fmt | `Column` | 指定格式转时间戳 | `df.withColumn("ts", to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"));` |
| `date_format` | Column dateExpr, String format | `Column` | 格式化日期 | `df.withColumn("formatted", date_format(col("date"), "yyyy年MM月dd日"));` |
| `from_unixtime` | Column ut | `Column` | Unix时间戳转字符串 | `df.withColumn("time_str", from_unixtime(col("unix_ts")));` |
| `unix_timestamp` | 无 | `Column` | 当前Unix时间戳 | `df.withColumn("ts", unix_timestamp());` |
| `unix_timestamp` | Column time | `Column` | 转为Unix时间戳 | `df.withColumn("unix", unix_timestamp(col("timestamp")));` |
| `unix_timestamp` | Column time, String fmt | `Column` | 指定格式转Unix时间戳 | `df.withColumn("unix", unix_timestamp(col("time_str"), "yyyy-MM-dd"));` |
| `array` | Column... cols | `Column` | 创建数组 | `df.withColumn("arr", array(col("a"), col("b")));` |
| `map` | Column... cols | `Column` | 创建Map | `df.withColumn("kv", map(col("key"), col("value")));` |
| `struct` | Column... cols | `Column` | 创建Struct | `df.withColumn("info", struct(col("name"), col("age")));` |
| `explode` | Column e | `Column` | 展开数组/Map为多行 | `df.select(col("id"), explode(col("tags")));` |
| `explode_outer` | Column e | `Column` | 展开数组/Map（保留null） | `df.select(col("id"), explode_outer(col("tags")));` |
| `posexplode` | Column e | `Column` | 展开数组并带位置 | `df.select(col("id"), posexplode(col("items")));` |
| `posexplode_outer` | Column e | `Column` | 展开数组带位置（保留null） | `df.select(col("id"), posexplode_outer(col("items")));` |
| `size` | Column e | `Column` | 数组/Map大小 | `df.withColumn("num_tags", size(col("tags")));` |
| `array_contains` | Column col, Object value | `Column` | 数组是否包含元素 | `df.filter(array_contains(col("tags"), "spark"));` |
| `sort_array` | Column e | `Column` | 数组排序（升序） | `df.withColumn("sorted", sort_array(col("arr")));` |
| `sort_array` | Column e, boolean asc | `Column` | 数组排序 | `df.withColumn("sorted", sort_array(col("arr"), false));` |
| `array_distinct` | Column e | `Column` | 数组去重 | `df.withColumn("unique", array_distinct(col("arr")));` |
| `array_intersect` | Column a1, Column a2 | `Column` | 数组交集 | `df.withColumn("common", array_intersect(col("arr1"), col("arr2")));` |
| `array_union` | Column a1, Column a2 | `Column` | 数组并集 | `df.withColumn("combined", array_union(col("arr1"), col("arr2")));` |
| `array_except` | Column a1, Column a2 | `Column` | 数组差集 | `df.withColumn("diff", array_except(col("arr1"), col("arr2")));` |
| `array_remove` | Column col, Object element | `Column` | 移除数组元素 | `df.withColumn("cleaned", array_remove(col("tags"), "old"));` |
| `array_position` | Column col, Object value | `Column` | 元素位置 | `df.withColumn("pos", array_position(col("arr"), "target"));` |
| `element_at` | Column col, Object extraction | `Column` | 获取数组/Map元素 | `df.withColumn("first", element_at(col("arr"), 1));` |
| `get_json_object` | Column e, String path | `Column` | 提取JSON字段 | `df.withColumn("name", get_json_object(col("json"), "$.name"));` |
| `json_tuple` | Column json, String... fields | `Column` | 提取多个JSON字段 | `df.select(json_tuple(col("json"), "name", "age"));` |
| `from_json` | Column col, Column schema | `Column` | JSON字符串转Struct | `df.withColumn("parsed", from_json(col("json_str"), schema));` |
| `to_json` | Column col | `Column` | Struct转JSON字符串 | `df.withColumn("json", to_json(col("struct_col")));` |
| `sha1` | Column e | `Column` | SHA1哈希 | `df.withColumn("hash", sha1(col("password")));` |
| `sha2` | Column e, int numBits | `Column` | SHA2哈希 | `df.withColumn("hash", sha2(col("password"), 256));` |
| `md5` | Column e | `Column` | MD5哈希 | `df.withColumn("hash", md5(col("content")));` |
| `crc32` | Column e | `Column` | CRC32哈希 | `df.withColumn("checksum", crc32(col("data")));` |
| `hash` | Column... cols | `Column` | 混合哈希 | `df.withColumn("hash", hash(col("id"), col("name")));` |
| `xxhash64` | Column... cols | `Column` | xxhash64哈希 | `df.withColumn("hash", xxhash64(col("id"), col("name")));` |
| `base64` | Column col | `Column` | Base64编码 | `df.withColumn("encoded", base64(col("data")));` |
| `unbase64` | Column col | `Column` | Base64解码 | `df.withColumn("decoded", unbase64(col("encoded")));` |
| `encode` | Column col, String charset | `Column` | 字符编码 | `df.withColumn("bytes", encode(col("text"), "UTF-8"));` |
| `decode` | Column col, String charset | `Column` | 字符解码 | `df.withColumn("text", decode(col("bytes"), "UTF-8"));` |
| `coalesce` | Column... e | `Column` | 返回第一个非null值 | `df.withColumn("name", coalesce(col("nickname"), col("fullname"), lit("N/A")));` |
| `ifnull` | Column col1, Column col2 | `Column` | 如果null返回第二个 | `df.withColumn("name", ifnull(col("name"), lit("Unknown")));` |
| `nullif` | Column col1, Column col2 | `Column` | 如果相等返回null | `df.withColumn("diff", nullif(col("a"), col("b")));` |
| `nvl` | Column col1, Column col2 | `Column` | NVL函数 | `df.withColumn("value", nvl(col("value"), lit(0)));` |
| `isnan` | Column e | `Column` | 判断是否NaN | `df.filter(isnan(col("score")));` |
| `nanvl` | Column col1, Column col2 | `Column` | 如果NaN返回第二个 | `df.withColumn("score", nanvl(col("score"), lit(0)));` |
| `monotonically_increasing_id` | 无 | `Column` | 生成单调递增ID | `df.withColumn("row_id", monotonically_increasing_id());` |
| `row_number` | 无 | `Column` | 行号（窗口函数） | `df.withColumn("row_num", row_number().over(Window.orderBy(col("id"))));` |
| `rank` | 无 | `Column` | 排名（有间隙） | `df.withColumn("rank", rank().over(Window.orderBy(col("score").desc())));` |
| `dense_rank` | 无 | `Column` | 排名（无间隙） | `df.withColumn("dense_rank", dense_rank().over(Window.orderBy(col("score").desc())));` |
| `percent_rank` | 无 | `Column` | 百分比排名 | `df.withColumn("pct", percent_rank().over(Window.orderBy(col("score"))));` |
| `lead` | Column e, int offset | `Column` | 向前N行 | `df.withColumn("next", lead(col("value"), 1).over(Window.orderBy(col("id"))));` |
| `lag` | Column e, int offset | `Column` | 向后N行 | `df.withColumn("prev", lag(col("value"), 1).over(Window.orderBy(col("id"))));` |
| `ntile` | int n | `Column` | 分桶 | `df.withColumn("bucket", ntile(4).over(Window.orderBy(col("score"))));` |
| `first_value` | Column e | `Column` | 窗口第一个值 | `df.withColumn("first", first_value(col("value")).over(Window.partitionBy("group")));` |
| `last_value` | Column e | `Column` | 窗口最后一个值 | `df.withColumn("last", last_value(col("value")).over(Window.partitionBy("group")));` |

### DataFrameReader
**包路径**: `org.apache.spark.sql`
**说明**: DataFrame读取器，用于从各种数据源读取数据。
**方法数量**: 15+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataFrameReader` | 指定数据源格式 | `spark.read().format("json").load("data.json");` |
| `option` | String key, String value | `DataFrameReader` | 设置选项（字符串） | `spark.read().option("header", "true").csv("data.csv");` |
| `option` | String key, boolean value | `DataFrameReader` | 设置选项（布尔） | `spark.read().option("multiline", true).json("data.json");` |
| `option` | String key, long value | `DataFrameReader` | 设置选项（长整数） | `spark.read().option("maxRowsPerFile", 10000L).format("csv");` |
| `options` | Map[String, String] options | `DataFrameReader` | 批量设置选项 | `Map<String, String> opts = new HashMap<>();<br>opts.put("header", "true");<br>spark.read().options(opts).csv("data.csv");` |
| `schema` | StructType schema | `DataFrameReader` | 指定schema | `StructType schema = DataTypes.createStructType(Arrays.asList(<br>    DataTypes.createStructField("id", DataTypes.IntegerType, true),<br>    DataTypes.createStructField("name", DataTypes.StringType, true)));<br>spark.read().schema(schema).csv("data.csv");` |
| `load` | 无 | `DataFrame` | 加载数据（用format指定格式） | `DataFrame df = spark.read().format("parquet").load("data.parquet");` |
| `load` | String path | `DataFrame` | 加载指定路径数据 | `DataFrame df = spark.read().format("json").load("data/*.json");` |
| `load` | String... paths | `DataFrame` | 加载多个路径数据 | `DataFrame df = spark.read().parquet("data1.parquet", "data2.parquet");` |
| `json` | String path | `DataFrame` | 读取JSON文件 | `DataFrame df = spark.read().json("data.json");` |
| `json` | Dataset[String] jsonDataset | `DataFrame` | 从Dataset读取JSON | `Dataset<String> jsonStrings = spark.createDataset(Arrays.asList("{\"id\":1}"), Encoders.STRING());<br>DataFrame df = spark.read().json(jsonStrings);` |
| `csv` | String path | `DataFrame` | 读取CSV文件 | `DataFrame df = spark.read().option("header", "true").csv("data.csv");` |
| `parquet` | String path | `DataFrame` | 读取Parquet文件 | `DataFrame df = spark.read().parquet("data.parquet");` |
| `orc` | String path | `DataFrame` | 读取ORC文件 | `DataFrame df = spark.read().orc("data.orc");` |
| `avro` | String path | `DataFrame` | 读取Avro文件 | `DataFrame df = spark.read().format("avro").load("data.avro");` |
| `text` | String path | `DataFrame` | 读取文本文件（每行一条记录） | `DataFrame df = spark.read().text("data.txt");` |
| `table` | String tableName | `DataFrame` | 从表读取数据 | `DataFrame df = spark.read().table("my_table");` |
| `jdbc` | String url, String table, Properties properties | `DataFrame` | 从JDBC读取数据 | `Properties props = new Properties();<br>props.put("user", "root");<br>props.put("password", "pwd");<br>DataFrame df = spark.read().jdbc("jdbc:mysql://localhost/db", "users", props);` |

### DataFrameWriter[T]
**包路径**: `org.apache.spark.sql`
**说明**: DataFrame写入器，用于将数据写入各种数据源。
**方法数量**: 20+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataFrameWriter[T]` | 指定输出格式 | `df.write().format("parquet").save("output");` |
| `option` | String key, String value | `DataFrameWriter[T]` | 设置选项（字符串） | `df.write().option("header", "true").csv("output");` |
| `option` | String key, boolean value | `DataFrameWriter[T]` | 设置选项（布尔） | `df.write().option("compression", "snappy").parquet("output");` |
| `options` | Map[String, String] options | `DataFrameWriter[T]` | 批量设置选项 | `Map<String, String> opts = new HashMap<>();<br>opts.put("header", "true");<br>df.write().options(opts).csv("output");` |
| `mode` | SaveMode mode | `DataFrameWriter[T]` | 设置写入模式 | `df.write().mode(SaveMode.Append).parquet("output");` |
| `mode` | String mode | `DataFrameWriter[T]` | 设置写入模式字符串 | `df.write().mode("overwrite").parquet("output");  // overwrite/append/ignore/errorIfExists` |
| `partitionBy` | String... colNames | `DataFrameWriter[T]` | 按列分区存储 | `df.write().partitionBy("year", "month").parquet("output");` |
| `bucketBy` | int numBuckets, String colName, String... colNames | `DataFrameWriter[T]` | 分桶存储 | `df.write().bucketBy(100, "id").sortBy("timestamp").saveAsTable("bucketed_table");` |
| `sortBy` | String... colNames | `DataFrameWriter[T]` | 分桶内排序 | `df.write().bucketBy(100, "id").sortBy("name").saveAsTable("sorted_table");` |
| `save` | 无 | `Unit` | 保存数据（用format指定格式） | `df.write().format("parquet").save();` |
| `save` | String path | `Unit` | 保存到指定路径 | `df.write().parquet("output/data.parquet");` |
| `saveAsTable` | String tableName | `Unit` | 保存为表 | `df.write().saveAsTable("my_table");` |
| `insertInto` | String tableName | `Unit` | 插入到表（不创建新表） | `df.write().insertInto("existing_table");` |
| `json` | String path | `Unit` | 写入JSON文件 | `df.write().json("output/data.json");` |
| `csv` | String path | `Unit` | 写入CSV文件 | `df.write().option("header", "true").csv("output/data.csv");` |
| `parquet` | String path | `Unit` | 写入Parquet文件 | `df.write().parquet("output/data.parquet");` |
| `orc` | String path | `Unit` | 写入ORC文件 | `df.write().orc("output/data.orc");` |
| `avro` | String path | `Unit` | 写入Avro文件 | `df.write().format("avro").save("output/data.avro");` |
| `text` | String path | `Unit` | 写入文本文件 | `df.select(col("text_col")).write().text("output/data.txt");` |
| `jdbc` | String url, String table, Properties connectionProperties | `Unit` | 写入JDBC表 | `Properties props = new Properties();<br>props.put("user", "root");<br>props.put("password", "pwd");<br>df.write().jdbc("jdbc:mysql://localhost/db", "users", props);` |

### Catalog
**包路径**: `org.apache.spark.sql.catalog`
**说明**: Spark Catalog接口，用于管理数据库、表、函数等元数据。
**方法数量**: 20+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `currentDatabase` | 无 | `String` | 获取当前数据库 | `String db = spark.catalog().currentDatabase();` |
| `setCurrentDatabase` | String db | `Unit` | 设置当前数据库 | `spark.catalog().setCurrentDatabase("my_db");` |
| `listDatabases` | 无 | `Dataset[Database]` | 列出所有数据库 | `spark.catalog().listDatabases().show();` |
| `listTables` | 无 | `Dataset[Table]` | 列出当前数据库的所有表 | `spark.catalog().listTables().show();` |
| `listTables` | String dbName | `Dataset[Table]` | 列出指定数据库的所有表 | `spark.catalog().listTables("my_db").show();` |
| `listFunctions` | 无 | `Dataset[Function]` | 列出所有函数 | `spark.catalog().listFunctions().show();` |
| `listFunctions` | String dbName | `Dataset[Function]` | 列出指定数据库的函数 | `spark.catalog().listFunctions("my_db").show();` |
| `listColumns` | String tableName | `Dataset[Column]` | 列出表的所有列 | `spark.catalog().listColumns("my_table").show();` |
| `listColumns` | String dbName, String tableName | `Dataset[Column]` | 列出指定数据库表的列 | `spark.catalog().listColumns("my_db", "my_table").show();` |
| `getTable` | String dbName, String tableName | `Table` | 获取表详情 | `Table table = spark.catalog().getTable("my_db", "my_table");` |
| `getTable` | String tableName | `Table` | 获取当前数据库的表 | `Table table = spark.catalog().getTable("my_table");` |
| `databaseExists` | String dbName | `Boolean` | 检查数据库是否存在 | `boolean exists = spark.catalog().databaseExists("my_db");` |
| `tableExists` | String tableName | `Boolean` | 检查表是否存在（当前库） | `boolean exists = spark.catalog().tableExists("my_table");` |
| `tableExists` | String dbName, String tableName | `Boolean` | 检查指定库表是否存在 | `boolean exists = spark.catalog().tableExists("my_db", "my_table");` |
| `functionExists` | String functionName | `Boolean` | 检查函数是否存在 | `boolean exists = spark.catalog().functionExists("my_func");` |
| `functionExists` | String dbName, String functionName | `Boolean` | 检查指定库函数是否存在 | `boolean exists = spark.catalog().functionExists("my_db", "my_func");` |
| `createDatabase` | String dbName, boolean ignoreIfExists | `Unit` | 创建数据库 | `spark.catalog().createDatabase("new_db", true);` |
| `createDatabase` | String dbName, boolean ignoreIfExists, String comment | `Unit` | 创建数据库（带注释） | `spark.catalog().createDatabase("new_db", false, "My test database");` |
| `dropDatabase` | String dbName, boolean ignoreIfNotExists, boolean cascade | `Unit` | 删除数据库 | `spark.catalog().dropDatabase("old_db", true, false);` |
| `createTable` | String tableName, String path | `Unit` | 创建表（指定路径） | `spark.catalog().createTable("new_table", "hdfs://data/path");` |
| `createTable` | String tableName, String path, String source | `Unit` | 创建表（指定格式） | `spark.catalog().createTable("new_table", "hdfs://data", "parquet");` |
| `createExternalTable` | String tableName, String path | `DataFrame` | 创建外部表 | `DataFrame df = spark.catalog().createExternalTable("ext_table", "hdfs://data");` |
| `createExternalTable` | String tableName, String path, String source | `DataFrame` | 创建外部表（指定格式） | `DataFrame df = spark.catalog().createExternalTable("ext_table", "hdfs://data", "parquet");` |
| `dropTable` | String dbName, String tableName, boolean ignoreIfNotExists, boolean purge | `Unit` | 删除表 | `spark.catalog().dropTable("my_db", "old_table", true, false);` |
| `dropTable` | String tableName, boolean ignoreIfNotExists, boolean purge | `Unit` | 删除当前库表 | `spark.catalog().dropTable("old_table", true, false);` |
| `dropTempView` | String viewName | `Unit` | 删除临时视图 | `spark.catalog().dropTempView("temp_view");` |
| `dropGlobalTempView` | String viewName | `Unit` | 删除全局临时视图 | `spark.catalog().dropGlobalTempView("global_view");` |
| `recoverPartitions` | String tableName | `Unit` | 恢复分区信息 | `spark.catalog().recoverPartitions("partitioned_table");` |
| `refreshTable` | String tableName | `Unit` | 刷新表缓存 | `spark.catalog().refreshTable("my_table");` |
| `refreshByPath` | String path | `Unit` | 刷新指定路径缓存 | `spark.catalog().refreshByPath("hdfs://data/table");` |
| `clearCache` | 无 | `Unit` | 清除所有缓存 | `spark.catalog().clearCache();` |
| `isCached` | String tableName | `Boolean` | 检查表是否被缓存 | `boolean cached = spark.catalog().isCached("my_table");` |
| `cacheTable` | String tableName | `Unit` | 缓存表 | `spark.catalog().cacheTable("my_table");` |
| `uncacheTable` | String tableName | `Unit` | 取消缓存表 | `spark.catalog().uncacheTable("my_table");` |

### UDFRegistration
**包路径**: `org.apache.spark.sql`
**说明**: UDF注册接口，用于注册用户自定义函数。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `register` | String name, UDF1[T1, R] f, DataType returnType | `void` | 注册UDF（1个参数） | `spark.udf().register("myUpper", (String s) -> s.toUpperCase(), DataTypes.StringType);` |
| `register` | String name, UDF2[T1, T2, R] f, DataType returnType | `void` | 注册UDF（2个参数） | `spark.udf().register("concat2", (String a, String b) -> a + b, DataTypes.StringType);` |
| `register` | String name, UDF3[T1, T2, T3, R] f, DataType returnType | `void` | 注册UDF（3个参数） | `spark.udf().register("combine3", (String a, String b, String c) -> a+b+c, DataTypes.StringType);` |
| `register` | String name, UDF4[T1, T2, T3, T4, R] f, DataType returnType | `void` | 注册UDF（4个参数） | - |
| `register` | String name, UDF5... | `void` | 注册UDF（5+参数） | - |
| `register` | String name, UDAF udaf | `void` | 注册聚合UDF | `spark.udf().register("mySum", new MySumUDAF());` |
| `register` | String name, UserDefinedAggregateFunction udaf | `void` | 注册聚合UDF（旧API） | - |
| `registerJava` | String name, String className, DataType returnType | `void` | 注册Java UDF类 | `spark.udf().registerJava("myFunc", "com.example.MyUDF", DataTypes.StringType);` |
| `registerPython` | String name, String command, DataType returnType | `void` | 注册Python UDF | - |
| `callUDF` | String udfName, Column... cols | `Column` | 调用已注册的UDF | `df.select(callUDF("myUpper", col("name")));` |
'''

def add_missing_api(filepath):
    """补充缺失的核心API"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 在文档末尾添加
    content = content.rstrip() + '\n' + MISSING_CORE_API + '\n'
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return True

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充缺失的核心API...")
    success = add_missing_api(filepath)
    
    if success:
        print("成功补充:")
        print("  - SparkConf配置类")
        print("  - Broadcast广播变量")
        print("  - Accumulator累加器")
        print("  - Column列表达式")
        print("  - functions内置函数（100+个）")
        print("  - DataFrameReader/Writer")
        print("  - Catalog元数据管理")
        print("  - UDFRegistration自定义函数注册")