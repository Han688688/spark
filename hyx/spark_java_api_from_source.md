# Apache Spark Java API Complete Reference

> This document provides a comprehensive list of all Java APIs in Apache Spark.

## Overview

- **Total Packages:** 15
- **Total Classes/Interfaces/Enums:** 50
- **Total Methods:** 849
- **Total Fields/Constants:** 1060

---

## Stability Annotations

The following stability annotations are used in Spark:

- `@Stable` - API is stable and unlikely to change
- `@Evolving` - API may evolve in future releases
- `@Experimental` - API is experimental and may change
- `@DeveloperApi` - API intended for developers, may change
- `@Deprecated` - API is deprecated, avoid using

---

## Table of Contents

- [org.apache.spark.api.java](#package-orgapachesparkapijava)
- [org.apache.spark.graphx](#package-orgapachesparkgraphx)
- [org.apache.spark.graphx.impl](#package-orgapachesparkgraphximpl)
- [org.apache.spark.sql.avro](#package-orgapachesparksqlavro)
- [org.apache.spark.sql.connector.read](#package-orgapachesparksqlconnectorread)
- [org.apache.spark.sql.connector.write](#package-orgapachesparksqlconnectorwrite)
- [org.apache.spark.sql.execution](#package-orgapachesparksqlexecution)
- [org.apache.spark.sql.execution.columnar](#package-orgapachesparksqlexecutioncolumnar)
- [org.apache.spark.sql.execution.datasources](#package-orgapachesparksqlexecutiondatasources)
- [org.apache.spark.sql.execution.datasources.orc](#package-orgapachesparksqlexecutiondatasourcesorc)
- [org.apache.spark.sql.execution.datasources.parquet](#package-orgapachesparksqlexecutiondatasourcesparquet)
- [org.apache.spark.sql.execution.vectorized](#package-orgapachesparksqlexecutionvectorized)
- [org.apache.spark.sql.internal](#package-orgapachesparksqlinternal)
- [org.apache.spark.streaming](#package-orgapachesparkstreaming)
- [org.apache.spark.streaming.util](#package-orgapachesparkstreamingutil)

---

## Package: `org.apache.spark.api.java`

**Classes in this package:** 1

### Quick Reference

- 🔷 `StorageLevels` - No description

---

### CLASS: `StorageLevels`

**Full Qualified Name:** `org.apache.spark.api.java.StorageLevels`

**Source File:** `core/src/main/java/org/apache/spark/api/java/StorageLevels.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `NONE` | `final StorageLevel` |  |
| `DISK_ONLY` | `final StorageLevel` |  |
| `DISK_ONLY_2` | `final StorageLevel` |  |
| `DISK_ONLY_3` | `final StorageLevel` |  |
| `MEMORY_ONLY` | `final StorageLevel` |  |
| `MEMORY_ONLY_2` | `final StorageLevel` |  |
| `MEMORY_ONLY_SER` | `final StorageLevel` |  |
| `MEMORY_ONLY_SER_2` | `final StorageLevel` |  |
| `MEMORY_AND_DISK` | `final StorageLevel` |  |
| `MEMORY_AND_DISK_2` | `final StorageLevel` |  |
| `MEMORY_AND_DISK_SER` | `final StorageLevel` |  |
| `MEMORY_AND_DISK_SER_2` | `final StorageLevel` |  |
| `OFF_HEAP` | `final StorageLevel` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `create` | `StorageLevel` | `boolean useDisk,
    boolean useMemory,
` |  | Expose some commonly useful storage level constants. / public class StorageLevel |

---

## Package: `org.apache.spark.graphx`

**Classes in this package:** 1

### Quick Reference

- 🔷 `TripletFields` - No description

---

### CLASS: `TripletFields`

**Full Qualified Name:** `org.apache.spark.graphx.TripletFields`

**Source File:** `graphx/src/main/java/org/apache/spark/graphx/TripletFields.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `useSrc` | `boolean` | Represents a subset of the fields of an [[EdgeTriplet]] or [ |
| `useDst` | `boolean` | Indicates whether the destination vertex attribute is includ |
| `useEdge` | `boolean` | Indicates whether the edge attribute is included. |
| `EdgeOnly` | `final TripletFields` | Expose only the edge field and not the source or destination |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `TripletFields` | `public` | `` |  | Represents a subset of the fields of an [[EdgeTriplet]] or [[EdgeContext]]. This |
| `TripletFields` | `public` | `boolean useSrc, boolean useDst, boolean ` |  |  |

---

## Package: `org.apache.spark.graphx.impl`

**Classes in this package:** 1

### Quick Reference

- ⭐ `EdgeActiveness` - No description

---

### ENUM: `EdgeActiveness`

**Full Qualified Name:** `org.apache.spark.graphx.impl.EdgeActiveness`

**Source File:** `graphx/src/main/java/org/apache/spark/graphx/impl/EdgeActiveness.java`

---

## Package: `org.apache.spark.sql.avro`

**Classes in this package:** 1

### Quick Reference

- 🔷 `from` - No description

---

### CLASS: `from`

**Full Qualified Name:** `org.apache.spark.sql.avro.from`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/avro/AvroCompressionCodec.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `codecName` | `String` |  |
| `supportCompressionLevel` | `boolean` |  |
| `codecNameMap` | `final EnumMap<AvroCompressionCodec, String>` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `AvroCompressionCodec` | `` | `String codecName,
      boolean supportC` |  |  |
| `getCodecName` | `String` | `` |  |  |
| `getSupportCompressionLevel` | `boolean` | `` |  |  |
| `fromString` | `AvroCompressionCodec` | `String s` |  |  |
| `lowerCaseName` | `String` | `` |  |  |

---

## Package: `org.apache.spark.sql.connector.read`

**Classes in this package:** 1

### Quick Reference

- 🔶 `is` - No description

---

### INTERFACE: `is`

**Full Qualified Name:** `org.apache.spark.sql.connector.read.is`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/connector/read/V1Scan.java`

---

## Package: `org.apache.spark.sql.connector.write`

**Classes in this package:** 1

### Quick Reference

- 🔶 `V1Write` - No description

---

### INTERFACE: `V1Write`

**Full Qualified Name:** `org.apache.spark.sql.connector.write.V1Write`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/connector/write/V1Write.java`

---

## Package: `org.apache.spark.sql.execution`

**Classes in this package:** 5

### Quick Reference

- 🔷 `RecordBinaryComparator` - No description
- 🔷 `UnsafeExternalRowSorter` - No description
- 🔷 `UnsafeFixedWidthAggregationMap` - No description
- 🔷 `for` - No description
- 🔶 `used` - No description

---

### CLASS: `RecordBinaryComparator`

**Full Qualified Name:** `org.apache.spark.sql.execution.RecordBinaryComparator`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/RecordBinaryComparator.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `UNALIGNED` | `final boolean` |  |
| `LITTLE_ENDIAN` | `final boolean` |  |
| `i` | `int` |  |
| `v1` | `int` |  |
| `v2` | `int` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `compare` | `int` | `Object leftObj, long leftOff, int leftLe` | @Override |  |
| `if` | `` | `leftLen != rightLen` |  |  |
| `if` | `` | `v1 != v2` |  |  |
| `while` | `` | `i <= leftLen - 8` |  |  |
| `if` | `` | `LITTLE_ENDIAN` |  |  |
| `while` | `` | `i < leftLen` |  |  |

---

### CLASS: `UnsafeExternalRowSorter`

**Full Qualified Name:** `org.apache.spark.sql.execution.UnsafeExternalRowSorter`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/UnsafeExternalRowSorter.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `numRowsInserted` | `long` |  |
| `schema` | `StructType` |  |
| `prefixComputer` | `PrefixComputer` |  |
| `sorter` | `UnsafeExternalSorter` |  |
| `isReleased` | `boolean` |  |
| `value` | `long` | Key prefix value, or the null prefix value if isNull = true. |
| `isNull` | `boolean` | Whether the key is null. |
| `recordComparatorSupplier` | `Supplier<RecordComparator>` |  |
| `sparkEnv` | `SparkEnv` |  |
| `taskContext` | `TaskContext` |  |
| `testSpillFrequency` | `` |  |
| `prefix` | `Prefix` |  |
| `sortedIterator` | `UnsafeSorterIterator` |  |
| `numFields` | `int` |  |
| `row` | `UnsafeRow` |  |
| `false` | `t keep references to the base object
              return` |  |
| `e` | `throw` |  |
| `ordering` | `Ordering<InternalRow>` |  |
| `row1` | `UnsafeRow` |  |
| `row2` | `UnsafeRow` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `computePrefix` | `Prefix` | `InternalRow row` |  | If positive, forces records to be spilled to disk at the given frequency (measur |
| `createWithRecordComparator` | `UnsafeExternalRowSorter` | `StructType schema,
      Supplier<Record` |  |  |
| `create` | `UnsafeExternalRowSorter` | `StructType schema,
      Ordering<Intern` |  |  |
| `UnsafeExternalRowSorter` | `private` | `StructType schema,
      Supplier<Record` |  |  |
| `setTestSpillFrequency` | `void` | `int frequency` | @VisibleForTesting | Forces spills to occur every `frequency` records. Only for use in tests. |
| `insertRow` | `void` | `UnsafeRow row` |  |  |
| `getPeakMemoryUsage` | `long` | `` |  | Return the peak memory used so far, in bytes. |
| `getSortTimeNanos` | `long` | `` |  |  |
| `cleanupResources` | `void` | `` |  |  |
| `sort` | `Iterator<InternalRow>` | `` |  |  |
| `RowIterator` | `return new` | `` |  |  |
| `advanceNext` | `boolean` | `` | @Override |  |
| `getRow` | `UnsafeRow` | `` | @Override |  |
| `sort` | `Iterator<InternalRow>` | `Iterator<UnsafeRow> inputIterator` |  |  |
| `RowComparator` | `` | `Ordering<InternalRow> ordering, int numF` |  |  |
| `compare` | `int` | `Object baseObj1,
        long baseOff1,
` | @Override |  |

---

### CLASS: `UnsafeFixedWidthAggregationMap`

**Full Qualified Name:** `org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/UnsafeFixedWidthAggregationMap.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `emptyAggregationBuffer` | `byte[]` | Unsafe-based HashMap for performing aggregations where the a |
| `aggregationBufferSchema` | `StructType` |  |
| `groupingKeySchema` | `StructType` |  |
| `groupingKeyProjection` | `UnsafeProjection` | Encodes grouping keys as UnsafeRows. |
| `map` | `BytesToBytesMap` | A hashmap which maps from opaque bytearray keys to bytearray |
| `currentAggregationBuffer` | `UnsafeRow` | Re-used pointer to the current aggregation buffer |
| `bytes` | `pageSizeBytes the data page size, in` |  |
| `valueProjection` | `Initialize the buffer for aggregation value
    final UnsafeProjection` |  |
| `unsafeGroupingKeyRow` | `UnsafeRow` |  |
| `loc` | `Location` |  |
| `putSucceeded` | `boolean` |  |
| `mapLocationIterator` | `MapIterator` |  |
| `key` | `UnsafeRow` |  |
| `value` | `UnsafeRow` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `supportsAggregationBufferSchema` | `boolean` | `StructType schema` |  | Unsafe-based HashMap for performing aggregations where the aggregated values are |
| `UnsafeFixedWidthAggregationMap` | `public` | `InternalRow emptyAggregationBuffer,
    ` |  | Create a new UnsafeFixedWidthAggregationMap. |
| `getAggregationBuffer` | `UnsafeRow` | `InternalRow groupingKey` |  | Return the aggregation buffer for the current group. For efficiency, all calls t |
| `getAggregationBufferFromUnsafeRow` | `UnsafeRow` | `UnsafeRow key` |  |  |
| `getAggregationBufferFromUnsafeRow` | `UnsafeRow` | `UnsafeRow key, int hash` |  |  |
| `if` | `` | `!putSucceeded` |  |  |
| `iterator` | `KVIterator<UnsafeRow, UnsafeRow>` | `` |  | Returns an iterator over the keys and values in this map. This uses destructive  |
| `next` | `boolean` | `` | @Override |  |
| `getKey` | `UnsafeRow` | `` | @Override |  |
| `getValue` | `UnsafeRow` | `` | @Override |  |
| `close` | `void` | `` | @Override |  |
| `getPeakMemoryUsedBytes` | `long` | `` |  | Return the peak memory used so far, in bytes. |
| `free` | `void` | `` |  | Free the memory associated with this map. This is idempotent and can be called m |
| `getAvgHashProbesPerKey` | `double` | `` |  | Gets the average number of hash probes per key lookup in the underlying `BytesTo |
| `destructAndCreateExternalSorter` | `UnsafeKVExternalSorter` | `` |  | Sorts the map's records in place, spill them to disk, and returns an [[UnsafeKVE |

---

### CLASS: `for`

**Full Qualified Name:** `org.apache.spark.sql.execution.for`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/UnsafeKVExternalSorter.java`

---

### INTERFACE: `used`

**Full Qualified Name:** `org.apache.spark.sql.execution.used`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/BufferedRowIterator.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `currentRows` | `LinkedList<InternalRow>` |  |
| `unsafeRow` | `used when there is no column in output
  protected UnsafeRow` |  |
| `startTimeNs` | `long` |  |
| `partitionIndex` | `int` |  |
| `IOException` | `throws` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `durationMs` | `long` | `` |  | An iterator interface used to pull the output from generated function for multip |
| `append` | `void` | `InternalRow row` |  | Append a row to currentRows. |
| `shouldStop` | `boolean` | `` |  | Returns whether `processNext()` should stop processing next row from `input` or  |
| `incPeakExecutionMemory` | `void` | `long size` |  | Increase the peak execution memory for current task. |
| `init` | `void` | `int index, Iterator<InternalRow>[] iters` |  | An iterator interface used to pull the output from generated function for multip |
| `processNext` | `void` | `` |  | Processes the input until have a row as output (currentRow). After it's called,  |

---

## Package: `org.apache.spark.sql.execution.columnar`

**Classes in this package:** 1

### Quick Reference

- 🔷 `ColumnDictionary` - No description

---

### CLASS: `ColumnDictionary`

**Full Qualified Name:** `org.apache.spark.sql.execution.columnar.ColumnDictionary`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/columnar/ColumnDictionary.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `intDictionary` | `int[]` |  |
| `longDictionary` | `long[]` |  |
| `floatDictionary` | `float[]` |  |
| `doubleDictionary` | `double[]` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `ColumnDictionary` | `public` | `int[] dictionary` |  |  |
| `ColumnDictionary` | `public` | `long[] dictionary` |  |  |
| `ColumnDictionary` | `public` | `float[] dictionary` |  |  |
| `ColumnDictionary` | `public` | `double[] dictionary` |  |  |
| `decodeToInt` | `int` | `int id` | @Override |  |
| `decodeToLong` | `long` | `int id` | @Override |  |
| `decodeToFloat` | `float` | `int id` | @Override |  |
| `decodeToDouble` | `double` | `int id` | @Override |  |
| `decodeToBinary` | `byte[]` | `int id` | @Override |  |

---

## Package: `org.apache.spark.sql.execution.datasources`

**Classes in this package:** 1

### Quick Reference

- 🔷 `SchemaColumnConvertNotSupportedException` - No description

---

### CLASS: `SchemaColumnConvertNotSupportedException`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/SchemaColumnConvertNotSupportedException.java`

**Stability:** @Unstable

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `column` | `String` | Exception thrown when the parquet reader find column type mi |
| `physicalType` | `String` | Physical column type in the actual parquet file. |
| `logicalType` | `String` | Logical column type in the parquet schema the parquet reader |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `getColumn` | `String` | `` |  |  |
| `getPhysicalType` | `String` | `` |  |  |
| `getLogicalType` | `String` | `` |  |  |
| `SchemaColumnConvertNotSupportedException` | `public` | `String column,
      String physicalType` |  |  |

---

## Package: `org.apache.spark.sql.execution.datasources.orc`

**Classes in this package:** 8

### Quick Reference

- 🔷 `OrcArrayColumnVector` - No description
- 🔷 `OrcAtomicColumnVector` - No description
- 🔷 `OrcMapColumnVector` - No description
- 🔷 `OrcStructColumnVector` - No description
- 🔷 `for` - No description
- 🔷 `from` - No description
- 🔷 `which` - No description
- 🔶 `wrapping` - No description

---

### CLASS: `OrcArrayColumnVector`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.orc.OrcArrayColumnVector`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/orc/OrcArrayColumnVector.java`

---

### CLASS: `OrcAtomicColumnVector`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.orc.OrcAtomicColumnVector`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/orc/OrcAtomicColumnVector.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `isTimestamp` | `boolean` |  |
| `isDate` | `boolean` |  |
| `longData` | `LongColumnVector` |  |
| `doubleData` | `DoubleColumnVector` |  |
| `bytesData` | `BytesColumnVector` |  |
| `decimalData` | `DecimalColumnVector` |  |
| `timestampData` | `TimestampColumnVector` |  |
| `value` | `int` |  |
| `index` | `int` |  |
| `data` | `BigDecimal` |  |
| `col` | `BytesColumnVector` |  |
| `binary` | `byte[]` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `OrcAtomicColumnVector` | `` | `DataType type, ColumnVector vector` |  |  |
| `if` | `` | `type instanceof TimestampType` |  |  |
| `if` | `` | `type instanceof DateType` |  |  |
| `if` | `` | `vector instanceof LongColumnVector longC` |  |  |
| `getBoolean` | `boolean` | `int rowId` | @Override |  |
| `getByte` | `byte` | `int rowId` | @Override |  |
| `getShort` | `short` | `int rowId` | @Override |  |
| `getInt` | `int` | `int rowId` | @Override |  |
| `if` | `` | `isDate` |  |  |
| `getLong` | `long` | `int rowId` | @Override |  |
| `if` | `` | `isTimestamp` |  |  |
| `getFloat` | `float` | `int rowId` | @Override |  |
| `getDouble` | `double` | `int rowId` | @Override |  |
| `getDecimal` | `Decimal` | `int rowId, int precision, int scale` | @Override |  |
| `getUTF8String` | `UTF8String` | `int rowId` | @Override |  |
| `getBinary` | `byte[]` | `int rowId` | @Override |  |
| `getArray` | `ColumnarArray` | `int rowId` | @Override |  |
| `getMap` | `ColumnarMap` | `int rowId` | @Override |  |
| `getChild` | `ColumnVector` | `int ordinal` |  |  |

---

### CLASS: `OrcMapColumnVector`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.orc.OrcMapColumnVector`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/orc/OrcMapColumnVector.java`

---

### CLASS: `OrcStructColumnVector`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.orc.OrcStructColumnVector`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/orc/OrcStructColumnVector.java`

---

### CLASS: `for`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.orc.for`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/orc/OrcColumnVectorUtils.java`

---

### CLASS: `from`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.orc.from`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/orc/OrcCompressionCodec.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `compressionKind` | `CompressionKind` |  |
| `codecNameMap` | `final EnumMap<OrcCompressionCodec, String>` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `OrcCompressionCodec` | `` | `CompressionKind compressionKind` |  |  |
| `getCompressionKind` | `CompressionKind` | `` |  |  |
| `lowerCaseName` | `String` | `` |  |  |

---

### CLASS: `which`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.orc.which`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/orc/OrcFooterReader.java`

---

### INTERFACE: `wrapping`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.orc.wrapping`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/orc/OrcColumnStatistics.java`

---

## Package: `org.apache.spark.sql.execution.datasources.parquet`

**Classes in this package:** 17

### Quick Reference

- 🔷 `ParquetColumnVector` - No description
- 🔷 `ParquetDictionary` - No description
- 🔷 `ParquetRowGroupReaderImpl` - Base class for custom RecordReaders for Parquet that directly materialize to `T`
- 🔶 `ParquetVectorUpdater` - No description
- 🔷 `ParquetVectorUpdaterFactory` - No description
- 🔷 `VectorizedColumnReader` - No description
- 🔷 `VectorizedDeltaBinaryPackedReader` - No description
- 🔷 `VectorizedDeltaByteArrayReader` - No description
- 🔷 `VectorizedDeltaLengthByteArrayReader` - No description
- 🔷 `VectorizedPlainValuesReader` - No description
- 🔷 `VectorizedRleValuesReader` - No description
- 🔶 `VectorizedValuesReader` - No description
- 🔶 `WKBConverterStrategy` - No description
- 🔷 `for` - No description
- 🔷 `from` - No description
- 🔷 `to` - No description
- 🔷 `which` - No description

---

### CLASS: `ParquetColumnVector`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.ParquetColumnVector`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetColumnVector.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `repetitionLevels` | `WritableColumnVector` | Contains necessary information representing a Parquet column |
| `definitionLevels` | `WritableColumnVector` |  |
| `columnReader` | `VectorizedColumnReader` | Reader for this column - only set if 'isPrimitive' is true |
| `fileContentCol` | `ParquetColumn` |  |
| `fileContent` | `WritableColumnVector` |  |
| `contentVector` | `ParquetColumnVector` |  |
| `variantSchema` | `` |  |
| `fieldsToExtract` | `` |  |
| `allChildrenAreMissing` | `boolean` |  |
| `i` | `int` |  |
| `childColumn` | `ParquetColumn` |  |
| `childVector` | `WritableColumnVector` |  |
| `childCv` | `ParquetColumnVector` |  |
| `result` | `List<ParquetColumnVector>` |  |
| `type` | `DataType` |  |
| `maxDefinitionLevel` | `int` |  |
| `maxElementRepetitionLevel` | `int` |  |
| `rowId` | `int` |  |
| `offset` | `0,` |  |
| `definitionLevel` | `int` |  |
| `length` | `int` |  |
| `maxRepetitionLevel` | `int` |  |
| `hasRepetitionLevels` | `boolean` |  |
| `0` | `>` |  |
| `break` | `` |  |
| `size` | `int` |  |
| `element` | `required int32` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `getChildren` | `List<ParquetColumnVector>` | `` |  | Contains necessary information representing a Parquet column, either of primitiv |
| `getLeaves` | `List<ParquetColumnVector>` | `` |  | Returns all the leaf columns in depth-first order. |
| `getLeavesHelper` | `void` | `ParquetColumnVector vector, List<Parquet` |  |  |
| `if` | `` | `vector.isPrimitive` |  |  |
| `for` | `` | `ParquetColumnVector child : vector.child` |  |  |
| `assemble` | `void` | `` |  | Assembles this column and calculate collection offsets recursively. This is a no |
| `if` | `` | `variantSchema != null` |  |  |
| `if` | `` | `fieldsToExtract == null` |  |  |
| `if` | `` | `type instanceof ArrayType || type instan` |  |  |
| `for` | `` | `ParquetColumnVector child : children` |  |  |
| `reset` | `void` | `` |  | Resets this Parquet column vector, which includes resetting all the writable col |
| `if` | `` | `repetitionLevels != null` |  |  |
| `if` | `` | `definitionLevels != null` |  |  |
| `getColumn` | `ParquetColumn` | `` |  | Returns the {@link ParquetColumn} of this column vector. |
| `getValueVector` | `WritableColumnVector` | `` |  | Returns the writable column vector used to store values. |
| `getRepetitionLevelVector` | `WritableColumnVector` | `` |  | Returns the writable column vector used to store repetition levels. |
| `getDefinitionLevelVector` | `WritableColumnVector` | `` |  | Returns the writable column vector used to store definition levels. |
| `getColumnReader` | `VectorizedColumnReader` | `` |  | Returns the column reader for reading a Parquet column. |
| `setColumnReader` | `void` | `VectorizedColumnReader reader` |  | Sets the column vector to 'reader'. Note this can only be called on a primitive  |
| `if` | `` | `!isPrimitive` |  |  |
| `assembleCollection` | `void` | `` |  | Assemble collections, e.g., array, map. |
| `if` | `` | `definitionLevel <= maxDefinitionLevel` |  |  |
| `if` | `` | `definitionLevel <= maxDefinitionLevel - ` |  |  |
| `assembleStruct` | `void` | `` |  |  |
| `getNextCollectionStart` | `int` | `int maxRepetitionLevel, int idx` |  | For a collection (i.e., array or map) element at index 'idx', returns the starti |
| `getCollectionSize` | `int` | `int maxRepetitionLevel, int idx` |  | Gets the size of a collection (i.e., array or map) element, starting at 'idx'. |
| `col` | `optional group` | `LIST` |  |  |
| `element` | `optional group` | `LIST` |  |  |

---

### CLASS: `ParquetDictionary`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.ParquetDictionary`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetDictionary.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `dictionary` | `Dictionary` |  |
| `needTransform` | `boolean` |  |
| `signed` | `long` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `ParquetDictionary` | `public` | `org.apache.parquet.column.Dictionary dic` |  |  |
| `decodeToInt` | `int` | `int id` | @Override |  |
| `if` | `` | `needTransform` |  |  |
| `decodeToLong` | `long` | `int id` | @Override |  |
| `decodeToFloat` | `float` | `int id` | @Override |  |
| `decodeToDouble` | `double` | `int id` | @Override |  |
| `decodeToBinary` | `byte[]` | `int id` | @Override |  |

---

### CLASS: `ParquetRowGroupReaderImpl`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.ParquetRowGroupReaderImpl`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/SpecificParquetRecordReaderBase.java`

**Description:**

Base class for custom RecordReaders for Parquet that directly materialize to `T`. This class handles computing row groups, filtering on them, setting up the column readers, etc.

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `reader` | `ParquetFileReader` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `ParquetRowGroupReaderImpl` | `` | `ParquetFileReader reader` |  |  |
| `readNextRowGroup` | `PageReadStore` | `` | @Override |  |
| `close` | `void` | `` | @Override |  |
| `if` | `` | `reader != null` |  |  |

---

### INTERFACE: `ParquetVectorUpdater`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.ParquetVectorUpdater`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetVectorUpdater.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `i` | `int` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `decodeDictionaryIds` | `default void` | `int total,
      int offset,
      Writa` |  | Process a batch of `total` values starting from `offset` in `values`, whose null |
| `for` | `` | `int i = offset; i < offset + total; i++` |  |  |
| `readValues` | `void` | `int total,
      int offset,
      Writa` |  | Read a batch of `total` values from `valuesReader` into `values`, starting from  |
| `skipValues` | `void` | `int total, VectorizedValuesReader values` |  | Skip a batch of `total` values from `valuesReader`. |
| `readValue` | `void` | `int offset, WritableColumnVector values,` |  | Read a single value from `valuesReader` into `values`, at `offset`. |
| `decodeSingleDictionaryId` | `void` | `int offset,
      WritableColumnVector v` |  | Process a batch of `total` values starting from `offset` in `values`, whose null |

---

### CLASS: `ParquetVectorUpdaterFactory`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.ParquetVectorUpdaterFactory`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetVectorUpdaterFactory.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `UTC` | `final ZoneId` |  |
| `logicalTypeAnnotation` | `LogicalTypeAnnotation` |  |
| `convertTz` | `ZoneId` |  |
| `datetimeRebaseMode` | `String` |  |
| `datetimeRebaseTz` | `String` |  |
| `int96RebaseMode` | `String` |  |
| `int96RebaseTz` | `String` |  |
| `type` | `PrimitiveType` |  |
| `typeName` | `PrimitiveTypeName` |  |
| `isUnknownType` | `boolean` |  |
| `UnknownLogicalTypeAnnotation` | `instanceof` |  |
| `failIfRebase` | `boolean` |  |
| `arrayLen` | `int` |  |
| `i` | `int` |  |
| `total` | `i <` |  |
| `days` | `long` |  |
| `rebasedDays` | `int` |  |
| `julianDays` | `int` |  |
| `bytes` | `byte[]` |  |
| `signed` | `long` |  |
| `unsigned` | `byte[]` |  |
| `timeZone` | `String` |  |
| `julianMicros` | `long` |  |
| `gregorianMillis` | `long` |  |
| `julianMillis` | `long` |  |
| `micros` | `long` |  |
| `v` | `Binary` |  |
| `srid` | `int` |  |
| `value` | `BigInteger` |  |
| `gregorianMicros` | `Read 12 bytes for INT96
      long` |  |
| `adjTime` | `long` |  |
| `sparkType` | `DecimalType` |  |
| `scaledDecimal` | `BigDecimal` |  |
| `parquetScale` | `int` |  |
| `typeAnnotation` | `LogicalTypeAnnotation` |  |
| `decimal` | `BigDecimal` |  |
| `DateLogicalTypeAnnotation` | `return typeAnnotation instanceof` |  |
| `intAnnotation` | `IntLogicalTypeAnnotation` |  |
| `requestedType` | `DecimalType` |  |
| `parquetType` | `DecimalLogicalTypeAnnotation` |  |
| `scaleIncrease` | `int` |  |
| `precisionIncrease` | `int` |  |
| `integerPrecision` | `int` |  |
| `d` | `DecimalType` |  |
| `decimalType` | `DecimalLogicalTypeAnnotation` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `ParquetVectorUpdaterFactory` | `` | `LogicalTypeAnnotation logicalTypeAnnotat` |  |  |
| `getUpdater` | `ParquetVectorUpdater` | `ColumnDescriptor descriptor, DataType sp` |  |  |
| `if` | `` | `isUnknownType && sparkType instanceof Nu` |  |  |
| `switch` | `` | `typeName` |  |  |
| `if` | `` | `sparkType == DataTypes.BooleanType` |  |  |
| `if` | `` | `sparkType == DataTypes.FloatType` |  |  |
| `if` | `` | `sparkType == DataTypes.DoubleType` |  |  |
| `if` | `` | `sparkType == DataTypes.TimestampNTZType` |  |  |
| `isTimestampTypeMatched` | `boolean` | `LogicalTypeAnnotation.TimeUnit unit` |  |  |
| `isTimeTypeMatched` | `boolean` | `LogicalTypeAnnotation.TimeUnit unit` |  |  |
| `isUnsignedIntTypeMatched` | `boolean` | `int bitWidth` |  |  |
| `constructConvertNotSupportedException` | `SchemaColumnConvertNotSupportedException` | `ColumnDescriptor descriptor,
      DataT` |  | Updater should not be called if all values are nulls, so all methods throw excep |
| `canReadAsIntDecimal` | `boolean` | `ColumnDescriptor descriptor, DataType dt` |  |  |
| `canReadAsLongDecimal` | `boolean` | `ColumnDescriptor descriptor, DataType dt` |  |  |
| `canReadAsBinaryDecimal` | `boolean` | `ColumnDescriptor descriptor, DataType dt` |  |  |
| `canReadAsDecimal` | `boolean` | `ColumnDescriptor descriptor, DataType dt` |  |  |
| `isLongDecimal` | `boolean` | `DataType dt` |  |  |
| `if` | `` | `dt instanceof DecimalType d` |  |  |
| `isDateTypeMatched` | `boolean` | `ColumnDescriptor descriptor` |  |  |
| `isSignedIntAnnotation` | `boolean` | `LogicalTypeAnnotation typeAnnotation` |  |  |
| `isDecimalTypeMatched` | `boolean` | `ColumnDescriptor descriptor, DataType dt` |  |  |
| `if` | `` | `typeAnnotation instanceof DecimalLogical` |  |  |
| `isSameDecimalScale` | `boolean` | `ColumnDescriptor descriptor, DataType dt` |  |  |
| `readValues` | `void` | `int total,
        int offset,
        W` | @Override |  |
| `skipValues` | `void` | `int total, VectorizedValuesReader values` | @Override |  |
| `readValue` | `void` | `int offset,
        WritableColumnVector` | @Override |  |
| `decodeSingleDictionaryId` | `void` | `int offset,
        WritableColumnVector` | @Override |  |
| `writeDecimal` | `void` | `int offset, WritableColumnVector values,` |  |  |

---

### CLASS: `VectorizedColumnReader`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedColumnReader.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `dictionary` | `Dictionary` | Decoder to return values from a single column. / public clas |
| `isCurrentPageDictionaryEncoded` | `boolean` | If true, the current page is dictionary encoded. |
| `dataColumn` | `ValuesReader` | Value readers. |
| `defColumn` | `VectorizedRleValuesReader` | Vectorized RLE decoder for definition levels |
| `repColumn` | `VectorizedRleValuesReader` | Vectorized RLE decoder for repetition levels |
| `updaterFactory` | `ParquetVectorUpdaterFactory` | Factory to get type-specific vector updater. |
| `readState` | `ParquetReadState` | Helper struct to track intermediate states while reading Par |
| `pageFirstRowIndex` | `long` | The index for the first row in the current page, among all r |
| `pageReader` | `PageReader` |  |
| `descriptor` | `ColumnDescriptor` |  |
| `logicalTypeAnnotation` | `LogicalTypeAnnotation` |  |
| `datetimeRebaseMode` | `String` |  |
| `writerVersion` | `ParsedVersion` |  |
| `dictionaryPage` | `DictionaryPage` |  |
| `isSupported` | `boolean` |  |
| `isDecimal` | `boolean` |  |
| `DecimalType` | `sparkType instanceof` |  |
| `needsUpcast` | `boolean` |  |
| `sparkType` | `` |  |
| `needsRebase` | `boolean` |  |
| `break` | `` |  |
| `isGeoType` | `boolean` |  |
| `GeographyType` | `sparkType instanceof` |  |
| `typeAnnotation` | `LogicalTypeAnnotation` |  |
| `parquetDecimal` | `DecimalLogicalTypeAnnotation` |  |
| `sparkDecimal` | `DecimalType` |  |
| `dictionaryIds` | `WritableColumnVector` |  |
| `updater` | `ParquetVectorUpdater` |  |
| `pageValueCount` | `int` |  |
| `pages` | `ve read all the` |  |
| `typeName` | `PrimitiveTypeName` |  |
| `startOffset` | `int` |  |
| `startRowId` | `Save starting row index so we can check if we need to eagerly decode dict ids later
        long` |  |
| `primitiveType` | `PrimitiveType` |  |
| `castLongToInt` | `boolean` |  |
| `isUnsignedInt32` | `signed int first
          boolean` |  |
| `isUnsignedInt64` | `signed long first
          boolean` |  |
| `needTransform` | `boolean` |  |
| `valuesReader` | `VectorizedValuesReader` |  |
| `page` | `DataPage` |  |
| `previousReader` | `ValuesReader` |  |
| `plainDict` | `Encoding` |  |
| `rlBitWidth` | `int` |  |
| `dlBitWidth` | `int` |  |
| `bytes` | `BytesInput` |  |
| `in` | `ByteBufferInputStream` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `needsDecimalScaleRebase` | `boolean` | `DataType sparkType` |  | Decoder to return values from a single column. / public class VectorizedColumnRe |
| `readBatch` | `void` | `int total,
      WritableColumnVector co` |  | Reads `total` rows from this columnReader into column. |
| `if` | `` | `dictionary != null` |  |  |
| `while` | `` | `readState.rowsToReadInBatch > 0 || !read` |  |  |
| `if` | `` | `readState.valuesToReadInPage == 0` |  |  |
| `if` | `` | `pageValueCount < 0` |  |  |
| `if` | `` | `isCurrentPageDictionaryEncoded` |  |  |
| `if` | `` | `readState.maxRepetitionLevel == 0` |  |  |
| `readPage` | `int` | `` |  |  |
| `if` | `` | `page == null` |  |  |
| `visit` | `Integer` | `DataPageV1 dataPageV1` | @Override |  |
| `visit` | `Integer` | `DataPageV2 dataPageV2` | @Override |  |
| `initDataReader` | `void` | `int pageValueCount,
      Encoding dataE` |  |  |
| `if` | `` | `dictionary == null` |  |  |
| `if` | `var to allow warning suppression` | `dataEncoding != plainDict && dataEncodin` |  |  |
| `getValuesReader` | `ValuesReader` | `Encoding encoding` |  |  |
| `if` | `` | `typeName == BOOLEAN` |  |  |
| `readPageV1` | `int` | `DataPageV1 page` |  |  |
| `readPageV2` | `int` | `DataPageV2 page` |  |  |

---

### CLASS: `VectorizedDeltaBinaryPackedReader`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.VectorizedDeltaBinaryPackedReader`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedDeltaBinaryPackedReader.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `href` | `<a` |  |
| `blockSizeInValues` | `header data
  private int` |  |
| `miniBlockNumInABlock` | `int` |  |
| `totalValueCount` | `int` |  |
| `firstValue` | `long` |  |
| `miniBlockSizeInValues` | `int` |  |
| `valuesRead` | `values read by the caller
  private int` |  |
| `lastValueRead` | `variables to keep state of the current block and miniblock
  private long` |  |
| `minDeltaInCurrentBlock` | `needed to compute the next value
  private long` |  |
| `currentMiniBlock` | `bitWidths array
  private int` |  |
| `bitWidths` | `int[]` |  |
| `remainingInBlock` | `bit widths for each miniBlock in the current block
  private int` |  |
| `remainingInMiniBlock` | `values in current block still to be read
  private int` |  |
| `unpackedValuesBuffer` | `values in current mini block still to be read
  private long[]` |  |
| `in` | `ByteBufferInputStream` |  |
| `byteVal` | `temporary buffers used by readByte, readShort, readInteger, and readLong
  private byte` |  |
| `shortVal` | `short` |  |
| `intVal` | `int` |  |
| `longVal` | `long` |  |
| `miniSize` | `double` |  |
| `remaining` | `int` |  |
| `n` | `int` |  |
| `i` | `int` |  |
| `outValue` | `calculate values from deltas unpacked for current block
      long` |  |
| `packer` | `BytePackerForLong` |  |
| `j` | `int` |  |
| `buffer` | `ByteBuffer` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `loadMiniBlockToOutput` | `int` | `int remaining, WritableColumnVector c, i` |  | An implementation of the Parquet DELTA_BINARY_PACKED decoder that supports the v |
| `if` | `read the block header` | `remainingInBlock == 0` |  |  |
| `if` | `new miniblock, unpack the miniblock` | `remainingInMiniBlock == 0` |  |  |
| `for` | `` | `int i = miniBlockSizeInValues - remainin` |  |  |
| `readBlockHeader` | `void` | `` |  |  |
| `unpackMiniBlock` | `void` | `` |  | mini block has a size of 8*n, unpack 32 value each time see org.apache.parquet.c |
| `for` | `` | `int j = 0; j < miniBlockSizeInValues; j ` |  |  |
| `readBitWidthsForMiniBlocks` | `DeltaBinaryPackingValuesReader
  private void` | `` |  |  |
| `for` | `` | `int i = 0; i < miniBlockNumInABlock; i++` |  |  |
| `skipValues` | `void` | `int total` |  |  |

---

### CLASS: `VectorizedDeltaByteArrayReader`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.VectorizedDeltaByteArrayReader`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedDeltaByteArrayReader.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `prefixLengthReader` | `VectorizedDeltaBinaryPackedReader` |  |
| `suffixReader` | `VectorizedDeltaLengthByteArrayReader` |  |
| `prefixLengthVector` | `WritableColumnVector` |  |
| `previous` | `ByteBuffer` |  |
| `currentRow` | `int` |  |
| `binaryValVector` | `Temporary variable used by readBinary
  private final WritableColumnVector` |  |
| `tempBinaryValVector` | `Temporary variable used by skipBinary
  private final WritableColumnVector` |  |
| `i` | `int` |  |
| `total` | `i <` |  |
| `prefixLength` | `int` |  |
| `suffix` | `ByteBuffer` |  |
| `suffixArray` | `byte[]` |  |
| `suffixLength` | `int` |  |
| `length` | `int` |  |
| `arrayData` | `We have to do this to materialize the output
      WritableColumnVector` |  |
| `offset` | `int` |  |
| `srid` | `int` |  |
| `wkb` | `byte[]` |  |
| `physicalValue` | `byte[]` |  |
| `c1` | `WritableColumnVector` |  |
| `c2` | `WritableColumnVector` |  |
| `tmp` | `WritableColumnVector` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `setPreviousReader` | `void` | `ValuesReader reader` | @Override | An implementation of the Parquet DELTA_BYTE_ARRAY decoder that supports the vect |
| `if` | `` | `reader != null` |  |  |
| `skipBinary` | `void` | `int total` | @Override |  |
| `for` | `` | `int i = 0; i < total; i++` |  |  |
| `if` | `` | `prefixLength != 0` |  |  |

---

### CLASS: `VectorizedDeltaLengthByteArrayReader`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.VectorizedDeltaLengthByteArrayReader`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedDeltaLengthByteArrayReader.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `lengthReader` | `VectorizedDeltaBinaryPackedReader` |  |
| `in` | `ByteBufferInputStream` |  |
| `lengthsVector` | `WritableColumnVector` |  |
| `currentRow` | `int` |  |
| `buffer` | `ByteBuffer` |  |
| `outputWriter` | `ByteBufferOutputWriter` |  |
| `length` | `int` |  |
| `i` | `int` |  |
| `total` | `i <` |  |
| `srid` | `int` |  |
| `physicalValue` | `byte[]` |  |
| `remaining` | `int` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `VectorizedDeltaLengthByteArrayReader` | `` | `` |  |  |
| `initFromPage` | `void` | `int valueCount, ByteBufferInputStream in` | @Override |  |
| `readBinary` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `for` | `` | `int i = 0; i < total; i++` |  |  |
| `readGeometry` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readGeography` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readGeoData` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `getBytes` | `ByteBuffer` | `int rowId` |  |  |
| `skipBinary` | `void` | `int total` | @Override |  |
| `while` | `` | `remaining > 0` |  |  |

---

### CLASS: `VectorizedPlainValuesReader`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.VectorizedPlainValuesReader`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedPlainValuesReader.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `in` | `ByteBufferInputStream` |  |
| `bitOffset` | `int` |  |
| `currentByte` | `byte` |  |
| `i` | `int` |  |
| `position` | `getBuffer returns a slice with` |  |
| `remaining` | `0 and` |  |
| `fullBytes` | `int` |  |
| `buffer` | `ByteBuffer` |  |
| `array` | `byte[]` |  |
| `offset` | `int` |  |
| `j` | `int` |  |
| `numBytesToSkip` | `int` |  |
| `requiredBytes` | `int` |  |
| `total` | `i <` |  |
| `rebase` | `boolean` |  |
| `scratch` | `, reused per batch
    byte[]` |  |
| `src` | `byte[]` |  |
| `data` | `copy 8 bytes per value
      byte[]` |  |
| `vector` | `c      the target column` |  |
| `bytes` | `provided reusable buffer of at least 9` |  |
| `msbIndex` | `int` |  |
| `needSignByte` | `boolean` |  |
| `valueLen` | `int` |  |
| `totalLen` | `int` |  |
| `scratchOffset` | `int` |  |
| `v` | `boolean` |  |
| `len` | `int` |  |
| `srid` | `int` |  |
| `base` | `int` |  |
| `dataLen` | `int` |  |
| `intSize` | `int` |  |
| `lenBuffer` | `ByteBuffer` |  |
| `out` | `ByteBufferOutputStream` |  |
| `physicalValue` | `byte[]` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `putLittleEndianBytesAsBigInteger` | `void` | `WritableColumnVector c, int rowId, byte[` |  | An implementation of the Parquet PLAIN decoder that supports the vectorized inte |
| `while` | `` | `msbIndex > offset && src[msbIndex] == 0` |  |  |
| `if` | `` | `msbIndex == offset && src[offset] == 0` |  |  |
| `if` | `` | `needSignByte` |  |  |
| `for` | `endian dest` | `int i = msbIndex; i >= offset; i--` |  |  |
| `readLongsWithRebase` | `void` | `int total,
      WritableColumnVector c,` | @Override |  |
| `for` | `` | `int i = 0; i < total; i += 1` |  |  |
| `if` | `` | `rebase` |  |  |
| `if` | `` | `failIfRebase` |  |  |
| `readFloats` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `skipFloats` | `void` | `int total` | @Override |  |
| `readDoubles` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `skipDoubles` | `void` | `int total` | @Override |  |
| `readBytes` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `for` | `` | `int i = 0; i < total; i++` |  |  |
| `skipBytes` | `void` | `int total` | @Override |  |
| `readShorts` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `skipShorts` | `void` | `int total` | @Override |  |
| `readBoolean` | `boolean` | `` | @Override |  |
| `if` | `` | `bitOffset == 0` |  |  |
| `if` | `` | `bitOffset == 8` |  |  |
| `readInteger` | `int` | `` | @Override |  |
| `readLong` | `long` | `` | @Override |  |
| `readByte` | `byte` | `` | @Override |  |
| `readShort` | `short` | `` | @Override |  |
| `readFloat` | `float` | `` | @Override |  |
| `readDouble` | `double` | `` | @Override |  |
| `readBinary` | `void` | `int total, WritableColumnVector v, int r` | @Override |  |
| `skipBinary` | `void` | `int total` | @Override |  |
| `readBinary` | `Binary` | `int len` | @Override |  |
| `skipFixedLenByteArray` | `void` | `int total, int len` | @Override |  |
| `readGeometry` | `void` | `int total, WritableColumnVector v, int r` | @Override |  |
| `readGeography` | `void` | `int total, WritableColumnVector v, int r` | @Override |  |
| `readGeoData` | `void` | `int total, WritableColumnVector v, int r` |  |  |

---

### CLASS: `VectorizedRleValuesReader`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.VectorizedRleValuesReader`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedRleValuesReader.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `in` | `ByteBufferInputStream` |  |
| `bitWidth` | `int` |  |
| `bytesWidth` | `int` |  |
| `packer` | `BytePacker` |  |
| `mode` | `Current decoding mode and values
  private MODE` |  |
| `currentCount` | `int` |  |
| `currentValue` | `int` |  |
| `currentBuffer` | `int[]` |  |
| `currentBufferIdx` | `int` |  |
| `fixedWidth` | `boolean` |  |
| `readLength` | `boolean` |  |
| `length` | `int` |  |
| `rowId` | `long` |  |
| `leftInBatch` | `int` |  |
| `leftInPage` | `int` |  |
| `n` | `int` |  |
| `rangeStart` | `long` |  |
| `rangeEnd` | `long` |  |
| `start` | `overlaps with the current row range in state
        long` |  |
| `end` | `long` |  |
| `toSkip` | `int` |  |
| `branch` | `counts from the RLE` |  |
| `maxDefLevel` | `int` |  |
| `bufEnd` | `int` |  |
| `valueOff` | `int` |  |
| `runStart` | `int` |  |
| `runLen` | `int` |  |
| `k` | `int` |  |
| `defLevelProcessor` | `DefLevelProcessor` |  |
| `valuesLeftInBlock` | `s left in the page
      int` |  |
| `i` | `int` |  |
| `break` | `` |  |
| `reader` | `VectorizedRleValuesReader` |  |
| `state` | `ParquetReadState` |  |
| `defLevels` | `WritableColumnVector` |  |
| `values` | `WritableColumnVector` |  |
| `nulls` | `WritableColumnVector` |  |
| `valuesReused` | `boolean` |  |
| `valueReader` | `VectorizedValuesReader` |  |
| `updater` | `ParquetVectorUpdater` |  |
| `initialValueOffset` | `int` |  |
| `num` | `int` |  |
| `valuesRead` | `int` |  |
| `levelIdx` | `int` |  |
| `runValue` | `int` |  |
| `totalSkipNum` | `int` |  |
| `left` | `int` |  |
| `value` | `int` |  |
| `shift` | `int` |  |
| `b` | `int` |  |
| `ch4` | `int` |  |
| `ch3` | `int` |  |
| `ch2` | `int` |  |
| `ch1` | `int` |  |
| `0` | `>` |  |
| `header` | `int` |  |
| `1` | `header >>>` |  |
| `numGroups` | `int` |  |
| `valueIndex` | `int` |  |
| `buffer` | `values are bit packed 8 at a time, so reading bitWidth will always work
            ByteBuffer` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `init` | `void` | `int bitWidth` |  | A values reader for Parquet's run-length encoded data. This is based off of the  |
| `readBoolean` | `boolean` | `` | @Override |  |
| `skip` | `void` | `` | @Override |  |
| `readValueDictionaryId` | `int` | `` | @Override |  |
| `readInteger` | `int` | `` | @Override |  |
| `if` | `` | `this.currentCount == 0` |  |  |
| `readBatch` | `void` | `ParquetReadState state,
      WritableCo` |  | Reads a batch of definition levels and values into vector 'defLevels' and 'value |
| `if` | `` | `defLevels == null` |  |  |
| `readIntegers` | `void` | `ParquetReadState state,
      WritableCo` |  | Decoding for dictionary ids. The IDs are populated into 'values' and the nullabi |
| `readBatchInternal` | `void` | `ParquetReadState state,
      WritableCo` |  |  |
| `while` | `` | `leftInBatch > 0 && leftInPage > 0` |  |  |
| `if` | `` | `rowId + n < rangeStart` |  |  |
| `if` | `` | `toSkip > 0` |  |  |
| `switch` | `` | `mode` |  |  |
| `if` | `` | `currentValue == state.maxDefinitionLevel` |  |  |
| `readPackedBatch` | `void` | `int n,
      ParquetReadState state,
   ` |  | PACKED-branch decode for {@link #readBatchInternal}. Extracted into a separate m |
| `while` | `` | `currentBufferIdx < bufEnd` |  |  |
| `if` | `` | `currentBuffer[currentBufferIdx] == maxDe` |  |  |
| `if` | `` | `runLen == 1` |  |  |
| `for` | `` | `int k = 0; k < runLen; k++` |  |  |
| `readBatchInternalWithDefLevels` | `void` | `ParquetReadState state,
      WritableCo` |  |  |
| `readBatchRepeated` | `void` | `ParquetReadState state,
      WritableCo` |  | Reads a batch of repetition levels, definition levels and values into 'repLevels |
| `readIntegersRepeated` | `void` | `ParquetReadState state,
      WritableCo` |  | Reads a batch of repetition levels, definition levels and integer values into 'r |
| `readBatchRepeatedInternal` | `void` | `ParquetReadState state,
      WritableCo` |  | Keep reading repetition level values from the page until either: 1) we've read e |
| `if` | `` | `currentValue == 0` |  |  |
| `if` | `` | `leftInBatch == 0` |  |  |
| `if` | `` | `n > 0` |  |  |
| `if` | `` | `!state.shouldSkip` |  |  |
| `for` | `` | `; i < valuesLeftInBlock; i++` |  |  |
| `DefLevelProcessor` | `` | `VectorizedRleValuesReader reader,
      ` |  |  |
| `readValues` | `void` | `int n` |  |  |
| `skipValues` | `void` | `int n` |  |  |
| `if` | `` | `state.shouldSkip` |  |  |
| `finish` | `void` | `` |  |  |
| `if` | `` | `state.numBatchedDefLevels > 0` |  |  |
| `readValues` | `void` | `int total,
      ParquetReadState state,` |  | Read the next 'total' values (either null or non-null) from this definition leve |
| `if` | `` | `!valuesReused` |  |  |
| `while` | `` | `n > 0` |  |  |
| `readValuesN` | `void` | `int n,
      ParquetReadState state,
   ` |  |  |
| `readPackedBatchWithDefLevels` | `void` | `int n,
      ParquetReadState state,
   ` |  | PACKED-branch decode for {@link #readValuesN}. Extracted for the same JIT reason |
| `while` | `` | `currentBufferIdx < end` |  |  |
| `if` | `` | `runValue == maxDefLevel` |  |  |
| `skipValues` | `void` | `int n,
      ParquetReadState state,
   ` |  | Skip the next `n` values (either null or non-null) from this definition level re |
| `for` | `` | `int i = 0; i < num; ++i` |  |  |
| `if` | `` | `currentBuffer[currentBufferIdx++] == sta` |  |  |
| `readIntegers` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `while` | `` | `left > 0` |  |  |
| `readUnsignedIntegers` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readUnsignedLongs` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readIntegersWithRebase` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readByte` | `byte` | `` | @Override |  |
| `readShort` | `short` | `` | @Override |  |
| `readBytes` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readShorts` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readLongs` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readLongsWithRebase` | `void` | `int total,
      WritableColumnVector c,` | @Override |  |
| `readBinary` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readGeometry` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readGeography` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readBooleans` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `for` | `` | `int i = 0; i < n; ++i` |  |  |
| `readFloats` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readDoubles` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readBinary` | `Binary` | `int len` | @Override |  |
| `skipIntegers` | `void` | `int total` | @Override |  |
| `skipBooleans` | `void` | `int total` | @Override |  |
| `skipBytes` | `void` | `int total` | @Override |  |
| `skipShorts` | `void` | `int total` | @Override |  |
| `skipLongs` | `void` | `int total` | @Override |  |
| `skipFloats` | `void` | `int total` | @Override |  |
| `skipDoubles` | `void` | `int total` | @Override |  |
| `skipBinary` | `void` | `int total` | @Override |  |
| `skipFixedLenByteArray` | `void` | `int total, int len` | @Override |  |
| `readUnsignedVarInt` | `int` | `` |  | Reads the next varint encoded int. |
| `readIntLittleEndian` | `int` | `` |  | Reads the next 4 byte little endian int. |
| `readIntLittleEndianPaddedOnBitWidth` | `int` | `` |  | Reads the next byteWidth little endian int. |
| `readNextGroup` | `boolean` | `` |  | Reads the next group. Returns false if no more group available. |
| `if` | `` | `this.currentBuffer.length < this.current` |  |  |
| `while` | `` | `valueIndex < this.currentCount` |  |  |

---

### INTERFACE: `VectorizedValuesReader`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.VectorizedValuesReader`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedValuesReader.java`

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `readBooleans` | `void` | `int total, WritableColumnVector c, int r` |  | Interface for value decoding that supports vectorized (aka batched) decoding. TO |
| `write` | `void` | `WritableColumnVector outputColumnVector,` |  | A functional interface to write integer values to columnar output / @FunctionalI |
| `writeArrayByteBuffer` | `void` | `WritableColumnVector c, int rowId, ByteB` |  |  |
| `skipWrite` | `void` | `WritableColumnVector c, int rowId, ByteB` |  |  |
| `readBytes` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readShorts` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readIntegers` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readIntegersWithRebase` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readUnsignedIntegers` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readUnsignedLongs` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readLongs` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readLongsWithRebase` | `void` | `int total,
      WritableColumnVector c,` |  |  |
| `readFloats` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readDoubles` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readBinary` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readGeometry` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `readGeography` | `void` | `int total, WritableColumnVector c, int r` |  |  |
| `skipBooleans` | `void` | `int total` |  |  |
| `skipBytes` | `void` | `int total` |  |  |
| `skipShorts` | `void` | `int total` |  |  |
| `skipIntegers` | `void` | `int total` |  |  |
| `skipLongs` | `void` | `int total` |  |  |
| `skipFloats` | `void` | `int total` |  |  |
| `skipDoubles` | `void` | `int total` |  |  |
| `skipBinary` | `void` | `int total` |  |  |
| `skipFixedLenByteArray` | `void` | `int total, int len` |  |  |

---

### INTERFACE: `WKBConverterStrategy`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.WKBConverterStrategy`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/WKBConverterStrategy.java`

---

### CLASS: `for`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.for`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedReaderBase.java`

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `skip` | `void` | `` | @Override |  |
| `readByte` | `byte` | `` | @Override |  |
| `readShort` | `short` | `` | @Override |  |
| `readBinary` | `Binary` | `int len` | @Override |  |
| `readBooleans` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readBytes` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readShorts` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readIntegers` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readIntegersWithRebase` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readUnsignedIntegers` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readUnsignedLongs` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readLongs` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readLongsWithRebase` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readFloats` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readDoubles` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readBinary` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readGeometry` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `readGeography` | `void` | `int total, WritableColumnVector c, int r` | @Override |  |
| `skipBooleans` | `void` | `int total` | @Override |  |
| `skipBytes` | `void` | `int total` | @Override |  |
| `skipShorts` | `void` | `int total` | @Override |  |
| `skipIntegers` | `void` | `int total` | @Override |  |
| `skipLongs` | `void` | `int total` | @Override |  |
| `skipFloats` | `void` | `int total` | @Override |  |
| `skipDoubles` | `void` | `int total` | @Override |  |
| `skipBinary` | `void` | `int total` | @Override |  |
| `skipFixedLenByteArray` | `void` | `int total, int len` | @Override |  |

---

### CLASS: `from`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.from`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetCompressionCodec.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `compressionCodec` | `CompressionCodecName` |  |
| `codecNameMap` | `final EnumMap<ParquetCompressionCodec, String>` |  |
| `availableCodecs` | `final List<ParquetCompressionCodec>` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `ParquetCompressionCodec` | `` | `CompressionCodecName compressionCodec` |  |  |
| `getCompressionCodec` | `CompressionCodecName` | `` |  |  |
| `fromString` | `ParquetCompressionCodec` | `String s` |  |  |
| `lowerCaseName` | `String` | `` |  |  |

---

### CLASS: `to`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.to`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetReadState.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `END_ROW_RANGE` | `final RowRange` | A special row range used when the row indexes are present AN |
| `rowRanges` | `Iterator<RowRange>` | Iterator over all row ranges, only not-null if column index  |
| `currentRange` | `RowRange` | The current row range |
| `maxRepetitionLevel` | `int` | Maximum repetition level for the Parquet column |
| `maxDefinitionLevel` | `int` | Maximum definition level for the Parquet column |
| `isRequired` | `boolean` | Whether this column is required |
| `rowId` | `long` | The current index over all rows within the column chunk. Thi |
| `valueOffset` | `int` | The offset in the current batch to put the next value in val |
| `levelOffset` | `int` | The offset in the current batch to put the next value in rep |
| `valuesToReadInPage` | `int` | The remaining number of values to read in the current page |
| `rowsToReadInBatch` | `int` | The remaining number of rows to read in the current batch |
| `lastListCompleted` | `boolean` | When processing repeated values, whether we've found the beg |
| `numBatchedDefLevels` | `int` | When processing repeated types, the number of accumulated de |
| `shouldSkip` | `boolean` | When processing repeated types, whether we should skip the c |
| `currentStart` | `long` |  |
| `previous` | `long` |  |
| `idx` | `long` |  |
| `range` | `RowRange` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `constructRanges` | `Iterator<RowRange>` | `PrimitiveIterator.OfLong rowIndexes` |  | Helper class to store intermediate state while reading a Parquet column chunk. / |
| `if` | `` | `rowIndexes == null` |  |  |
| `if` | `` | `currentStart == Long.MIN_VALUE` |  |  |
| `if` | `` | `previous != Long.MIN_VALUE` |  |  |
| `resetForNewBatch` | `void` | `int batchSize` |  | Must be called at the beginning of reading a new batch. |
| `resetForNewPage` | `void` | `int totalValuesInPage, long pageFirstRow` |  | Must be called at the beginning of reading a new page. |
| `currentRangeStart` | `long` | `` |  | Returns the start index of the current row range. |
| `currentRangeEnd` | `long` | `` |  | Returns the end index of the current row range. |
| `nextRange` | `void` | `` |  | Advance to the next range. |
| `if` | `` | `rowRanges == null` |  |  |
| `RowRange` | `record` | `long start, long end` |  | Helper struct to represent a range of row indexes `[start, end]`. |

---

### CLASS: `which`

**Full Qualified Name:** `org.apache.spark.sql.execution.datasources.parquet.which`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetFooterReader.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `groups` | `skipRowGroup If true, skip reading row` |  |
| `fileStart` | `long` |  |
| `readOptions` | `ParquetReadOptions` |  |
| `stream` | `var` |  |
| `inputFile` | `var` |  |
| `inputStream` | `var` |  |
| `fileReader` | `var` |  |
| `footer` | `var` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `openFileAndReadFooter` | `OpenedParquetFooter` | `Configuration hadoopConf,
      Partitio` |  | `ParquetFooterReader` is a util class which encapsulates the helper methods of r |
| `if` | `` | `keepInputStreamOpen` |  |  |

---

## Package: `org.apache.spark.sql.execution.vectorized`

**Classes in this package:** 8

### Quick Reference

- 🔷 `AggregateHashMap` - No description
- 🔷 `ColumnVectorUtils` - No description
- 🔷 `OffHeapColumnVector` - No description
- 🔷 `OnHeapColumnVector` - No description
- 🔷 `adds` - No description
- 🔷 `adds` - No description
- 🔶 `for` - No description
- 🔷 `intentionally` - No description

---

### CLASS: `AggregateHashMap`

**Full Qualified Name:** `org.apache.spark.sql.execution.vectorized.AggregateHashMap`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/AggregateHashMap.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `columnVectors` | `OnHeapColumnVector[]` |  |
| `aggBufferRow` | `MutableColumnarRow` |  |
| `buckets` | `int[]` |  |
| `numBuckets` | `int` |  |
| `numRows` | `int` |  |
| `maxSteps` | `int` |  |
| `DEFAULT_CAPACITY` | `int` |  |
| `16` | `1 <<` |  |
| `DEFAULT_LOAD_FACTOR` | `double` |  |
| `DEFAULT_MAX_STEPS` | `int` |  |
| `idx` | `int` |  |
| `h` | `long` |  |
| `step` | `int` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `AggregateHashMap` | `public` | `StructType schema, int capacity, double ` |  |  |
| `AggregateHashMap` | `public` | `StructType schema` |  |  |
| `findOrInsert` | `MutableColumnarRow` | `long key` |  |  |
| `if` | `` | `idx != -1 && buckets[idx] == -1` |  |  |
| `find` | `int` | `long key` | @VisibleForTesting |  |
| `while` | `` | `step < maxSteps` |  |  |
| `if` | `s either an empty slot or already contains the key` | `buckets[idx] == -1` |  |  |
| `hash` | `long` | `long key` |  |  |
| `equals` | `boolean` | `int idx, long key1` |  |  |

---

### CLASS: `ColumnVectorUtils`

**Full Qualified Name:** `org.apache.spark.sql.execution.vectorized.ColumnVectorUtils`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/ColumnVectorUtils.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `t` | `DataType` |  |
| `pdt` | `PhysicalDataType` |  |
| `v` | `UTF8String` |  |
| `d` | `Decimal` |  |
| `integer` | `BigInteger` |  |
| `bytes` | `byte[]` |  |
| `i` | `int` |  |
| `keys` | `int[]` |  |
| `values` | `int[]` |  |
| `result` | `Map<Integer, Integer>` |  |
| `b` | `byte[]` |  |
| `c` | `CalendarInterval` |  |
| `capacity` | `int` |  |
| `columnVectors` | `WritableColumnVector[]` |  |
| `n` | `int` |  |
| `r` | `Row` |  |
| `batch` | `ColumnarBatch` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `populate` | `void` | `ConstantColumnVector col, InternalRow ro` |  | Utilities to help manipulate data associate with ColumnVectors. These should be  |
| `if` | `` | `pdt instanceof PhysicalBooleanType` |  |  |
| `toJavaIntArray` | `int[]` | `ColumnarArray array` |  | Returns the array data as the java primitive array. For example, an array of Int |
| `toJavaIntMap` | `Map<Integer, Integer>` | `ColumnarMap map` |  |  |
| `for` | `` | `int i = 0; i < keys.length; i++` |  |  |
| `appendValue` | `void` | `WritableColumnVector dst, DataType t, Ob` |  |  |
| `if` | `` | `o == null` |  |  |
| `if` | `` | `t instanceof CalendarIntervalType || t i` |  |  |
| `if` | `` | `t == DataTypes.BooleanType` |  |  |
| `appendValue` | `void` | `WritableColumnVector dst, DataType t, Ro` |  |  |
| `if` | `` | `t instanceof ArrayType at` |  |  |
| `for` | `` | `Object o : values` |  |  |
| `toBatch` | `ColumnarBatch` | `StructType schema, MemoryMode memMode, I` |  | Converts an iterator of rows into a single ColumnBatch. |
| `if` | `` | `memMode == MemoryMode.OFF_HEAP` |  |  |

---

### CLASS: `OffHeapColumnVector`

**Full Qualified Name:** `org.apache.spark.sql.execution.vectorized.OffHeapColumnVector`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/OffHeapColumnVector.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `bigEndianPlatform` | `final boolean` |  |
| `vectors` | `OffHeapColumnVector[]` |  |
| `i` | `int` |  |
| `nulls` | `long` |  |
| `data` | `long` |  |
| `lengthData` | `long` |  |
| `offsetData` | `long` |  |
| `offset` | `long` |  |
| `count` | `i <` |  |
| `v` | `byte` |  |
| `rowId` | `putBooleans requires 8 slots available at` |  |
| `capacity` | `,` |  |
| `expanded` | `long` |  |
| `array` | `boolean[]` |  |
| `srcAddr` | `long` |  |
| `tmp` | `byte[]` |  |
| `srcOffset` | `int` |  |
| `dstOffset` | `long` |  |
| `bb` | `ByteBuffer` |  |
| `result` | `int` |  |
| `oldCapacity` | `int` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `allocateColumns` | `OffHeapColumnVector[]` | `int capacity, StructType schema` |  | Column data backed using offheap memory. / public final class OffHeapColumnVecto |
| `allocateColumns` | `OffHeapColumnVector[]` | `int capacity, StructField[] fields` |  | Allocates columns to store elements of each field off heap. Capacity is the init |
| `for` | `` | `int i = 0; i < fields.length; i++` |  |  |
| `OffHeapColumnVector` | `public` | `int capacity, DataType type` |  |  |
| `valuesNativeAddress` | `long` | `` | @VisibleForTesting | Returns the off heap pointer for the values buffer. |
| `releaseMemory` | `void` | `` |  |  |
| `close` | `void` | `` | @Override |  |
| `putNotNull` | `void` | `int rowId` | @Override |  |
| `putNull` | `void` | `int rowId` | @Override |  |
| `putNulls` | `void` | `int rowId, int count` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i, ++offset` |  |  |
| `putNotNulls` | `void` | `int rowId, int count` | @Override |  |
| `isNullAt` | `boolean` | `int rowId` | @Override |  |
| `putBoolean` | `void` | `int rowId, boolean value` | @Override |  |
| `putBooleans` | `void` | `int rowId, int count, boolean value` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i` |  |  |
| `putBooleans` | `void` | `int rowId, byte src` | @Override |  |
| `if` | `` | `bigEndianPlatform` |  |  |
| `getBoolean` | `boolean` | `int rowId` | @Override |  |
| `getBooleans` | `boolean[]` | `int rowId, int count` | @Override |  |
| `putByte` | `void` | `int rowId, byte value` | @Override |  |
| `putBytes` | `void` | `int rowId, int count, byte value` | @Override |  |
| `putBytes` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putBytes` | `void` | `int rowId, int count, ByteBuffer src, in` | @Override |  |
| `getByte` | `byte` | `int rowId` | @Override |  |
| `if` | `` | `dictionary == null` |  |  |
| `getBytes` | `byte[]` | `int rowId, int count` | @Override |  |
| `for` | `` | `int i = 0; i < count; i++` |  |  |
| `getBytesAsUTF8String` | `UTF8String` | `int rowId, int count` | @Override |  |
| `getByteBuffer` | `ByteBuffer` | `int rowId, int count` | @Override |  |
| `putShort` | `void` | `int rowId, short value` | @Override |  |
| `putShorts` | `void` | `int rowId, int count, short value` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i, offset += 2` |  |  |
| `putShorts` | `void` | `int rowId, int count, short[] src, int s` | @Override |  |
| `putShorts` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putShortsFromIntsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i, srcOffset += ` |  |  |
| `getShort` | `short` | `int rowId` | @Override |  |
| `getShorts` | `short[]` | `int rowId, int count` | @Override |  |
| `putInt` | `void` | `int rowId, int value` | @Override |  |
| `putInts` | `void` | `int rowId, int count, int value` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i, offset += 4` |  |  |
| `putInts` | `void` | `int rowId, int count, int[] src, int src` | @Override |  |
| `putInts` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putIntsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `if` | `` | `!bigEndianPlatform` |  |  |
| `for` | `` | `int i = 0; i < count; ++i, offset += 4, ` |  |  |
| `getInt` | `int` | `int rowId` | @Override |  |
| `getInts` | `int[]` | `int rowId, int count` | @Override |  |
| `getDictId` | `int` | `int rowId` | @Override | Returns the dictionary Id for rowId. This should only be called when the ColumnV |
| `putLong` | `void` | `int rowId, long value` | @Override |  |
| `putLongs` | `void` | `int rowId, int count, long value` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i, offset += 8` |  |  |
| `putLongs` | `void` | `int rowId, int count, long[] src, int sr` | @Override |  |
| `putLongs` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putLongsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i, offset += 8, ` |  |  |
| `getLong` | `long` | `int rowId` | @Override |  |
| `getLongs` | `long[]` | `int rowId, int count` | @Override |  |
| `putFloat` | `void` | `int rowId, float value` | @Override |  |
| `putFloats` | `void` | `int rowId, int count, float value` | @Override |  |
| `putFloats` | `void` | `int rowId, int count, float[] src, int s` | @Override |  |
| `putFloats` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putFloatsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `getFloat` | `float` | `int rowId` | @Override |  |
| `getFloats` | `float[]` | `int rowId, int count` | @Override |  |
| `putDouble` | `void` | `int rowId, double value` | @Override |  |
| `putDoubles` | `void` | `int rowId, int count, double value` | @Override |  |
| `putDoubles` | `void` | `int rowId, int count, double[] src, int ` | @Override |  |
| `putDoubles` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putDoublesLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `getDouble` | `double` | `int rowId` | @Override |  |
| `getDoubles` | `double[]` | `int rowId, int count` | @Override |  |
| `putArray` | `void` | `int rowId, int offset, int length` | @Override |  |
| `getArrayLength` | `int` | `int rowId` | @Override |  |
| `getArrayOffset` | `int` | `int rowId` | @Override |  |
| `putByteArray` | `int` | `int rowId, byte[] value, int offset, int` | @Override |  |
| `reserveInternal` | `void` | `int newCapacity` | @Override |  |
| `reserveNewColumn` | `OffHeapColumnVector` | `int capacity, DataType type` | @Override |  |

---

### CLASS: `OnHeapColumnVector`

**Full Qualified Name:** `org.apache.spark.sql.execution.vectorized.OnHeapColumnVector`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/OnHeapColumnVector.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `bigEndianPlatform` | `final boolean` |  |
| `vectors` | `OnHeapColumnVector[]` |  |
| `i` | `int` |  |
| `nulls` | `byte[]` |  |
| `byteData` | `byte[]` |  |
| `shortData` | `short[]` |  |
| `intData` | `int[]` |  |
| `longData` | `long[]` |  |
| `floatData` | `float[]` |  |
| `doubleData` | `double[]` |  |
| `arrayLengths` | `int[]` |  |
| `arrayOffsets` | `int[]` |  |
| `count` | `i <` |  |
| `v` | `byte` |  |
| `rowId` | `putBooleans requires 8 slots available at` |  |
| `capacity` | `,` |  |
| `expanded` | `long` |  |
| `array` | `boolean[]` |  |
| `srcOffset` | `int` |  |
| `bb` | `ByteBuffer` |  |
| `result` | `int` |  |
| `newLengths` | `int[]` |  |
| `newOffsets` | `int[]` |  |
| `newData` | `byte[]` |  |
| `newNulls` | `byte[]` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `allocateColumns` | `OnHeapColumnVector[]` | `int capacity, StructType schema` |  | A column backed by an in memory JVM array. This stores the NULLs as a byte per v |
| `allocateColumns` | `OnHeapColumnVector[]` | `int capacity, StructField[] fields` |  | Allocates columns to store elements of each field on heap. Capacity is the initi |
| `for` | `` | `int i = 0; i < fields.length; i++` |  |  |
| `OnHeapColumnVector` | `public` | `int capacity, DataType type` |  |  |
| `releaseMemory` | `void` | `` |  |  |
| `close` | `void` | `` | @Override |  |
| `putNotNull` | `void` | `int rowId` | @Override |  |
| `putNull` | `void` | `int rowId` | @Override |  |
| `putNulls` | `void` | `int rowId, int count` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i` |  |  |
| `putNotNulls` | `void` | `int rowId, int count` | @Override |  |
| `isNullAt` | `boolean` | `int rowId` | @Override |  |
| `putBoolean` | `void` | `int rowId, boolean value` | @Override |  |
| `putBooleans` | `void` | `int rowId, int count, boolean value` | @Override |  |
| `putBooleans` | `void` | `int rowId, byte src` | @Override |  |
| `if` | `` | `bigEndianPlatform` |  |  |
| `getBoolean` | `boolean` | `int rowId` | @Override |  |
| `getBooleans` | `boolean[]` | `int rowId, int count` | @Override |  |
| `putByte` | `void` | `int rowId, byte value` | @Override |  |
| `putBytes` | `void` | `int rowId, int count, byte value` | @Override |  |
| `putBytes` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putBytes` | `void` | `int rowId, int count, ByteBuffer src, in` | @Override |  |
| `getByte` | `byte` | `int rowId` | @Override |  |
| `if` | `` | `dictionary == null` |  |  |
| `getBytes` | `byte[]` | `int rowId, int count` | @Override |  |
| `for` | `` | `int i = 0; i < count; i++` |  |  |
| `getBytesAsUTF8String` | `UTF8String` | `int rowId, int count` | @Override |  |
| `getByteBuffer` | `ByteBuffer` | `int rowId, int count` | @Override |  |
| `putShort` | `void` | `int rowId, short value` | @Override |  |
| `putShorts` | `void` | `int rowId, int count, short value` | @Override |  |
| `putShorts` | `void` | `int rowId, int count, short[] src, int s` | @Override |  |
| `putShorts` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putShortsFromIntsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i, srcOffset += ` |  |  |
| `getShort` | `short` | `int rowId` | @Override |  |
| `getShorts` | `short[]` | `int rowId, int count` | @Override |  |
| `putInt` | `void` | `int rowId, int value` | @Override |  |
| `putInts` | `void` | `int rowId, int count, int value` | @Override |  |
| `putInts` | `void` | `int rowId, int count, int[] src, int src` | @Override |  |
| `putInts` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putIntsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `getInt` | `int` | `int rowId` | @Override |  |
| `getInts` | `int[]` | `int rowId, int count` | @Override |  |
| `getDictId` | `int` | `int rowId` | @Override | Returns the dictionary Id for rowId. This should only be called when the ColumnV |
| `putLong` | `void` | `int rowId, long value` | @Override |  |
| `putLongs` | `void` | `int rowId, int count, long value` | @Override |  |
| `putLongs` | `void` | `int rowId, int count, long[] src, int sr` | @Override |  |
| `putLongs` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putLongsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `for` | `` | `int i = 0; i < count; ++i, srcOffset += ` |  |  |
| `getLong` | `long` | `int rowId` | @Override |  |
| `getLongs` | `long[]` | `int rowId, int count` | @Override |  |
| `putFloat` | `void` | `int rowId, float value` | @Override |  |
| `putFloats` | `void` | `int rowId, int count, float value` | @Override |  |
| `putFloats` | `void` | `int rowId, int count, float[] src, int s` | @Override |  |
| `putFloats` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putFloatsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `if` | `` | `!bigEndianPlatform` |  |  |
| `getFloat` | `float` | `int rowId` | @Override |  |
| `getFloats` | `float[]` | `int rowId, int count` | @Override |  |
| `putDouble` | `void` | `int rowId, double value` | @Override |  |
| `putDoubles` | `void` | `int rowId, int count, double value` | @Override |  |
| `putDoubles` | `void` | `int rowId, int count, double[] src, int ` | @Override |  |
| `putDoubles` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `putDoublesLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` | @Override |  |
| `getDouble` | `double` | `int rowId` | @Override |  |
| `getDoubles` | `double[]` | `int rowId, int count` | @Override |  |
| `getArrayLength` | `int` | `int rowId` | @Override |  |
| `getArrayOffset` | `int` | `int rowId` | @Override |  |
| `putArray` | `void` | `int rowId, int offset, int length` | @Override |  |
| `putByteArray` | `int` | `int rowId, byte[] value, int offset, int` | @Override |  |
| `reserveInternal` | `void` | `int newCapacity` | @Override |  |
| `if` | `` | `this.arrayLengths != null` |  |  |
| `if` | `` | `byteData == null || byteData.length < ne` |  |  |
| `if` | `` | `shortData == null || shortData.length < ` |  |  |
| `if` | `` | `intData == null || intData.length < newC` |  |  |
| `if` | `` | `longData == null || longData.length < ne` |  |  |
| `if` | `` | `floatData == null || floatData.length < ` |  |  |
| `if` | `` | `doubleData == null || doubleData.length ` |  |  |
| `reserveNewColumn` | `OnHeapColumnVector` | `int capacity, DataType type` | @Override |  |

---

### CLASS: `adds`

**Full Qualified Name:** `org.apache.spark.sql.execution.vectorized.adds`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/WritableColumnVector.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `dictionaryIds` | `WritableColumnVector` | Reusable column for ids of dictionary. |
| `defaultCapacity` | `int` | The default number of rows that can be stored in this column |
| `MAX_CAPACITY` | `int` | Upper limit for the maximum capacity for this column. |
| `hugeVectorThreshold` | `int` |  |
| `hugeVectorReserveRatio` | `double` |  |
| `numNulls` | `int` | Number of nulls in this column. This is an optimization for  |
| `isConstant` | `boolean` | True if this column's values are fixed. This means the colum |
| `isMissing` | `boolean` | True if this column is missing from the file. This means the |
| `DEFAULT_ARRAY_LENGTH` | `final int` | Default size of each array length value. This grows as neces |
| `childType` | `DataType` |  |
| `childCapacity` | `int` |  |
| `i` | `int` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `reset` | `void` | `` |  | This class adds write APIs to ColumnVector. It supports all the types and contai |
| `if` | `` | `childColumns != null` |  |  |
| `for` | `` | `WritableColumnVector c: childColumns` |  |  |
| `if` | `` | `numNulls > 0` |  |  |
| `if` | `` | `hugeVectorThreshold > -1 && capacity > h` |  |  |
| `close` | `void` | `` | @Override |  |
| `for` | `` | `int i = 0; i < childColumns.length; i++` |  |  |
| `if` | `` | `dictionaryIds != null` |  |  |
| `closeIfFreeable` | `void` | `` | @Override |  |
| `reserveAdditional` | `void` | `int additionalCapacity` |  |  |
| `reserve` | `void` | `int requiredCapacity` |  |  |
| `if` | `` | `requiredCapacity < 0` |  |  |
| `if` | `` | `requiredCapacity <= newCapacity` |  |  |
| `throwUnsupportedException` | `void` | `int requiredCapacity, Throwable cause` |  |  |
| `hasNull` | `boolean` | `` | @Override |  |
| `numNulls` | `int` | `` | @Override |  |
| `hasDictionary` | `boolean` | `` |  | The Dictionary for this column. If it's not null, will be used to decode the val |
| `getDictionaryIds` | `WritableColumnVector` | `` |  | Returns the underlying integer column for ids of dictionary. |
| `setDictionary` | `void` | `Dictionary dictionary` |  | Update the dictionary. |
| `reserveDictionaryIds` | `WritableColumnVector` | `int capacity` |  | Reserve a integer column for ids of dictionary. |
| `if` | `` | `dictionaryIds == null` |  |  |
| `putBooleans` | `void` | `int rowId, int count, byte src, int srcI` |  | Sets bits from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)  |
| `putBytes` | `void` | `int rowId, int count, ByteBuffer src, in` |  | Copies {@code count} bytes from a {@link ByteBuffer} starting at absolute positi |
| `putByteArray` | `int` | `int rowId, byte[] value` |  |  |
| `putByteArray` | `int` | `int rowId, ByteBuffer src, int srcPositi` |  | Stores bytes from a {@link ByteBuffer} as a variable-length byte array at {@code |
| `appendBytes` | `int` | `int length, ByteBuffer src, int srcPosit` |  |  |
| `getDecimal` | `Decimal` | `int rowId, int precision, int scale` | @Override |  |
| `putDecimal` | `void` | `int rowId, Decimal value, int precision` |  |  |
| `putInterval` | `void` | `int rowId, CalendarInterval value` |  |  |
| `getUTF8String` | `UTF8String` | `int rowId` | @Override |  |
| `if` | `` | `dictionary == null` |  |  |
| `getBytesAsUTF8String` | `UTF8String` | `int rowId, int count` |  | Gets the values of bytes from [rowId, rowId + count), as a UTF8String. This meth |
| `getBinary` | `byte[]` | `int rowId` | @Override |  |
| `getByteBuffer` | `ByteBuffer` | `int rowId, int count` |  | Gets the values of bytes from [rowId, rowId + count), as a ByteBuffer. This meth |
| `appendNull` | `int` | `` |  | Append APIs. These APIs all behave similarly and will append data to the current |
| `appendNotNull` | `int` | `` |  |  |
| `appendNulls` | `int` | `int count` |  |  |
| `appendNotNulls` | `int` | `int count` |  |  |
| `appendBoolean` | `int` | `boolean v` |  |  |
| `appendBooleans` | `int` | `int count, boolean v` |  |  |
| `appendBooleans` | `int` | `int count, byte src, int offset` |  | Append bits from [src[offset], src[offset + count]) src must contain bit-packed  |
| `appendByte` | `int` | `byte v` |  |  |
| `appendBytes` | `int` | `int count, byte v` |  |  |
| `appendBytes` | `int` | `int length, byte[] src, int offset` |  |  |
| `appendShort` | `int` | `short v` |  |  |
| `appendShorts` | `int` | `int count, short v` |  |  |
| `appendShorts` | `int` | `int length, short[] src, int offset` |  |  |
| `appendInt` | `int` | `int v` |  |  |
| `appendInts` | `int` | `int count, int v` |  |  |
| `appendInts` | `int` | `int length, int[] src, int offset` |  |  |
| `appendLong` | `int` | `long v` |  |  |
| `appendLongs` | `int` | `int count, long v` |  |  |
| `appendLongs` | `int` | `int length, long[] src, int offset` |  |  |
| `appendFloat` | `int` | `float v` |  |  |
| `appendFloats` | `int` | `int count, float v` |  |  |
| `appendFloats` | `int` | `int length, float[] src, int offset` |  |  |
| `appendDouble` | `int` | `double v` |  |  |
| `appendDoubles` | `int` | `int count, double v` |  |  |
| `appendDoubles` | `int` | `int length, double[] src, int offset` |  |  |
| `appendByteArray` | `int` | `byte[] value, int offset, int length` |  |  |
| `appendArray` | `int` | `int length` |  |  |
| `for` | `` | `WritableColumnVector childColumn : child` |  |  |
| `appendStruct` | `int` | `boolean isNull` |  | Appends a NULL struct. This *has* to be used for structs instead of appendNull() |
| `if` | `` | `isNull` |  |  |
| `if` | `` | `c.type instanceof StructType || c.type i` |  |  |
| `appendObjects` | `Optional<Integer>` | `int length, Object value` |  | Appends multiple copies of a Java Object to the vector using the corresponding a |
| `if` | `` | `value instanceof Boolean` |  |  |
| `if` | `` | `value instanceof Byte` |  |  |
| `if` | `` | `value instanceof Decimal decimal` |  |  |
| `for` | `` | `int i = 0; i < length; ++i` |  |  |
| `if` | `` | `value instanceof Double` |  |  |
| `if` | `` | `value instanceof Float` |  |  |
| `if` | `` | `value instanceof Integer` |  |  |
| `if` | `` | `value instanceof Long` |  |  |
| `if` | `` | `value instanceof Short` |  |  |
| `if` | `` | `value instanceof UTF8String utf8` |  |  |
| `if` | `` | `value instanceof GenericArrayData arrayD` |  |  |
| `if` | `` | `value instanceof GenericInternalRow row` |  |  |
| `if` | `` | `value instanceof ArrayBasedMapData data` |  |  |
| `getArray` | `ColumnarArray` | `int rowId` | @Override |  |
| `getMap` | `ColumnarMap` | `int rowId` | @Override |  |
| `arrayData` | `WritableColumnVector` | `` |  |  |
| `getChild` | `WritableColumnVector` | `int ordinal` | @Override |  |
| `getNumChildren` | `int` | `` |  | Returns the number of child vectors. |
| `getElementsAppended` | `int` | `` |  | Returns the elements appended. This is useful |
| `addElementsAppended` | `void` | `int num` |  | Increment number of elements appended by 'num'. This is useful when one wants to |
| `setIsConstant` | `void` | `` |  | Marks this column as being constant. |
| `for` | `` | `WritableColumnVector c : childColumns` |  |  |
| `setMissing` | `void` | `` |  | Marks this column missing from the file. |
| `isMissing` | `boolean` | `` |  | Whether this column is missing from the file. |
| `isAllNull` | `boolean` | `` |  | Whether this column only contains null values. |
| `isArray` | `boolean` | `` |  |  |
| `expandBoolByteToLong` | `long` | `byte b` |  | Expands each bit of a bit-packed boolean byte into a separate byte within a long |
| `WritableColumnVector` | `protected` | `int capacity, DataType dataType` |  | Sets up the common state and also handles creating the child columns if this is  |
| `if` | `` | `type instanceof ArrayType` |  |  |
| `for` | `` | `int i = 0; i < childColumns.length; ++i` |  |  |
| `reserveInternal` | `void` | `int capacity` |  | Reserve a integer column for ids of dictionary. / public WritableColumnVector re |
| `putNotNull` | `void` | `int rowId` |  | Sets null/not null to the value at rowId. |
| `putNull` | `void` | `int rowId` |  |  |
| `putNulls` | `void` | `int rowId, int count` |  | Sets null/not null to the values at [rowId, rowId + count). |
| `putNotNulls` | `void` | `int rowId, int count` |  |  |
| `putBoolean` | `void` | `int rowId, boolean value` |  | Sets `value` to the value at rowId. |
| `putByte` | `void` | `int rowId, byte value` |  | Sets `value` to the value at rowId. |
| `putShort` | `void` | `int rowId, short value` |  | Sets `value` to the value at rowId. |
| `putShorts` | `void` | `int rowId, int count, short value` |  | Sets value to [rowId, rowId + count). |
| `putShortsFromIntsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` |  | Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + c |
| `putInt` | `void` | `int rowId, int value` |  | Sets `value` to the value at rowId. |
| `putInts` | `void` | `int rowId, int count, int value` |  | Sets value to [rowId, rowId + count). |
| `putIntsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` |  | Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + c |
| `putLong` | `void` | `int rowId, long value` |  | Sets `value` to the value at rowId. |
| `putLongs` | `void` | `int rowId, int count, long value` |  | Sets value to [rowId, rowId + count). |
| `putLongsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` |  | Sets values from [src + srcIndex, src + srcIndex + count * 8) to [rowId, rowId + |
| `putFloat` | `void` | `int rowId, float value` |  | Sets `value` to the value at rowId. |
| `putFloats` | `void` | `int rowId, int count, float value` |  | Sets value to [rowId, rowId + count). |
| `putFloatsLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` |  | Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + c |
| `putDouble` | `void` | `int rowId, double value` |  | Sets `value` to the value at rowId. |
| `putDoubles` | `void` | `int rowId, int count, double value` |  | Sets value to [rowId, rowId + count). |
| `putDoublesLittleEndian` | `void` | `int rowId, int count, byte[] src, int sr` |  | Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + c |
| `putArray` | `void` | `int rowId, int offset, int length` |  | Puts a byte array that already exists in this column. |

---

### CLASS: `adds`

**Full Qualified Name:** `org.apache.spark.sql.execution.vectorized.adds`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/ConstantColumnVector.java`

#### Fields & Constants

| Field Name | Type | Description |
|------------|------|-------------|
| `nullData` | `byte` |  |
| `byteData` | `byte` |  |
| `shortData` | `short` |  |
| `intData` | `int` |  |
| `longData` | `long` |  |
| `floatData` | `float` |  |
| `doubleData` | `double` |  |
| `stringData` | `UTF8String` |  |
| `byteArrayData` | `byte[]` |  |
| `childData` | `ConstantColumnVector[]` |  |
| `arrayData` | `ColumnarArray` |  |
| `mapData` | `ColumnarMap` |  |
| `numRows` | `int` |  |
| `i` | `int` |  |
| `bytes` | `byte[]` |  |
| `bigInteger` | `BigInteger` |  |
| `javaDecimal` | `BigDecimal` |  |
| `value` | `` |  |

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `ConstantColumnVector` | `public` | `int numRows, DataType type` |  | This class adds the constant support to ColumnVector. It supports all the types  |
| `if` | `` | `type instanceof StructType structType` |  |  |
| `closeIfFreeable` | `void` | `` |  |  |
| `close` | `void` | `` | @Override |  |
| `if` | `` | `childData != null` |  |  |
| `for` | `` | `int i = 0; i < childData.length; i++` |  |  |
| `if` | `` | `childData[i] != null` |  |  |
| `hasNull` | `boolean` | `` | @Override |  |
| `numNulls` | `int` | `` | @Override |  |
| `isNullAt` | `boolean` | `int rowId` | @Override |  |
| `setNull` | `void` | `` |  | Sets all rows as `null` |
| `setNotNull` | `void` | `` |  | Sets all rows as not `null` |
| `getBoolean` | `boolean` | `int rowId` | @Override |  |
| `setBoolean` | `void` | `boolean value` |  | Sets the boolean `value` for all rows |
| `getByte` | `byte` | `int rowId` | @Override |  |
| `setByte` | `void` | `byte value` |  | Sets the byte `value` for all rows |
| `getShort` | `short` | `int rowId` | @Override |  |
| `setShort` | `void` | `short value` |  | Sets the short `value` for all rows |
| `getInt` | `int` | `int rowId` | @Override |  |
| `setInt` | `void` | `int value` |  | Sets the int `value` for all rows |
| `getLong` | `long` | `int rowId` | @Override |  |
| `setLong` | `void` | `long value` |  | Sets the long `value` for all rows |
| `getFloat` | `float` | `int rowId` | @Override |  |
| `setFloat` | `void` | `float value` |  | Sets the float `value` for all rows |
| `getDouble` | `double` | `int rowId` | @Override |  |
| `setDouble` | `void` | `double value` |  | Sets the double `value` for all rows |
| `getArray` | `ColumnarArray` | `int rowId` | @Override |  |
| `setArray` | `void` | `ColumnarArray value` |  | Sets the `ColumnarArray` `value` for all rows |
| `getMap` | `ColumnarMap` | `int ordinal` | @Override |  |
| `setMap` | `void` | `ColumnarMap value` |  | Sets the `ColumnarMap` `value` for all rows |
| `getDecimal` | `Decimal` | `int rowId, int precision, int scale` | @Override |  |
| `setDecimal` | `void` | `Decimal value, int precision` |  | Sets the `Decimal` `value` with the precision for all rows |
| `getUTF8String` | `UTF8String` | `int rowId` | @Override |  |
| `setUtf8String` | `void` | `UTF8String value` |  | Sets the `UTF8String` `value` for all rows |
| `setByteArray` | `void` | `byte[] value` |  | Sets the byte array `value` for all rows |
| `getBinary` | `byte[]` | `int rowId` | @Override |  |
| `setBinary` | `void` | `byte[] value` |  | Sets the binary `value` for all rows |
| `getChild` | `ColumnVector` | `int ordinal` | @Override |  |
| `setChild` | `void` | `int ordinal, ConstantColumnVector value` |  | Sets the child `ConstantColumnVector` `value` at the given ordinal for all rows |
| `setCalendarInterval` | `void` | `CalendarInterval value` |  | Sets the CalendarInterval `value` for all rows |
| `setVariant` | `void` | `VariantVal value` |  | Sets the Variant `value` for all rows |

---

### INTERFACE: `for`

**Full Qualified Name:** `org.apache.spark.sql.execution.vectorized.for`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/Dictionary.java`

---

### CLASS: `intentionally`

**Full Qualified Name:** `org.apache.spark.sql.execution.vectorized.intentionally`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/MutableColumnarRow.java`

---

## Package: `org.apache.spark.sql.internal`

**Classes in this package:** 1

### Quick Reference

- 🔷 `loader` - No description

---

### CLASS: `loader`

**Full Qualified Name:** `org.apache.spark.sql.internal.loader`

**Source File:** `sql/core/src/main/java/org/apache/spark/sql/internal/NonClosableMutableURLClassLoader.java`

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `NonClosableMutableURLClassLoader` | `public` | `ClassLoader parent` |  |  |
| `super` | `` | `new URL[]{}, parent` |  |  |
| `close` | `void` | `` | @Override |  |

---

## Package: `org.apache.spark.streaming`

**Classes in this package:** 1

### Quick Reference

- ⭐ `StreamingContextState` - No description

---

### ENUM: `StreamingContextState`

**Full Qualified Name:** `org.apache.spark.streaming.StreamingContextState`

**Source File:** `streaming/src/main/java/org/apache/spark/streaming/StreamingContextState.java`

**Stability:** @DeveloperApi

---

## Package: `org.apache.spark.streaming.util`

**Classes in this package:** 2

### Quick Reference

- 🔷 `WriteAheadLog` - :: DeveloperApi :: This abstract class represents a write ahead log (aka journal
- 🔷 `WriteAheadLogRecordHandle` - :: DeveloperApi :: This abstract class represents a handle that refers to a reco

---

### CLASS: `WriteAheadLog`

**Full Qualified Name:** `org.apache.spark.streaming.util.WriteAheadLog`

**Source File:** `streaming/src/main/java/org/apache/spark/streaming/util/WriteAheadLog.java`

**Description:**

:: DeveloperApi :: This abstract class represents a write ahead log (aka journal) that is used by Spark Streaming to save the received data (by receivers) and associated metadata to a reliable storage, so that

#### Methods

| Method | Return Type | Parameters | Stability | Description |
|--------|-------------|------------|-----------|-------------|
| `clean` | `void` | `long threshTime, boolean waitForCompleti` |  | Write the record to the log and return a record handle, which contains all the i |
| `close` | `void` | `` |  | Close this log and release any resources. It must be idempotent. |

---

### CLASS: `WriteAheadLogRecordHandle`

**Full Qualified Name:** `org.apache.spark.streaming.util.WriteAheadLogRecordHandle`

**Source File:** `streaming/src/main/java/org/apache/spark/streaming/util/WriteAheadLogRecordHandle.java`

**Description:**

:: DeveloperApi :: This abstract class represents a handle that refers to a record written in a {@link org.apache.spark.streaming.util.WriteAheadLog WriteAheadLog}.

---

