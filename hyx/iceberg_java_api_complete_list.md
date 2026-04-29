# Apache Iceberg Java API Complete List

Version: Latest (1.10.1)
Generated: 2026-04-29
Reference: https://iceberg.apache.org/javadoc/latest/

---

## Overview

Apache Iceberg provides a comprehensive Java API for table management, data operations, and metadata handling. This document provides a complete inventory of all public API interfaces and methods.

---

## 1. Table API (org.apache.iceberg)

### 1.1 Table Interface (@Public)

**Description**: Represents an Iceberg table with operations for reading, writing, and evolving metadata.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| name | `default String name()` | Return the full name for this table | @Public |
| refresh | `void refresh()` | Refresh the current table metadata | @Public |
| newScan | `TableScan newScan()` | Create a new scan for this table | @Public |
| newBatchScan | `default BatchScan newBatchScan()` | Create a new batch scan for this table | @Public |
| newIncrementalAppendScan | `default IncrementalAppendScan newIncrementalAppendScan()` | Create incremental scan for appends | @Public |
| newIncrementalChangelogScan | `default IncrementalChangelogScan newIncrementalChangelogScan()` | Create incremental changelog scan | @Public |
| schema | `Schema schema()` | Return the schema for this table | @Public |
| schemas | `Map<Integer, Schema> schemas()` | Return a map of schemas for this table | @Public |
| spec | `PartitionSpec spec()` | Return the partition spec for this table | @Public |
| specs | `Map<Integer, PartitionSpec> specs()` | Return a map of partition specs | @Public |
| sortOrder | `SortOrder sortOrder()` | Return the sort order for this table | @Public |
| sortOrders | `Map<Integer, SortOrder> sortOrders()` | Return a map of sort orders | @Public |
| properties | `Map<String, String> properties()` | Return string properties for this table | @Public |
| location | `String location()` | Return the table's base location | @Public |
| currentSnapshot | `Snapshot currentSnapshot()` | Get the current snapshot for this table | @Public |
| snapshot | `Snapshot snapshot(long snapshotId)` | Get snapshot with given id | @Public |
| snapshot | `default Snapshot snapshot(String name)` | Get snapshot referenced by name | @Public |
| snapshots | `Iterable<Snapshot> snapshots()` | Get the snapshots of this table | @Public |
| history | `List<HistoryEntry> history()` | Get the snapshot history of this table | @Public |
| updateSchema | `UpdateSchema updateSchema()` | Create UpdateSchema to alter columns | @Public |
| updateSpec | `UpdatePartitionSpec updateSpec()` | Create UpdatePartitionSpec for partition changes | @Public |
| updateProperties | `UpdateProperties updateProperties()` | Create UpdateProperties for property changes | @Public |
| replaceSortOrder | `ReplaceSortOrder replaceSortOrder()` | Create ReplaceSortOrder for sort order changes | @Public |
| updateLocation | `UpdateLocation updateLocation()` | Create UpdateLocation for location changes | @Public |
| newAppend | `AppendFiles newAppend()` | Create append API to add files | @Public |
| newFastAppend | `default AppendFiles newFastAppend()` | Create fast append API | @Public |
| newRewrite | `RewriteFiles newRewrite()` | Create rewrite API to replace files | @Public |
| rewriteManifests | `RewriteManifests rewriteManifests()` | Create rewrite manifests API | @Public |
| newOverwrite | `OverwriteFiles newOverwrite()` | Create overwrite API by filter expression | @Public |
| newRowDelta | `RowDelta newRowDelta()` | Create row-level delta API | @Public |
| newReplacePartitions | `ReplacePartitions newReplacePartitions()` | Create replace partitions API (deprecated) | @Public |
| newDelete | `DeleteFiles newDelete()` | Create delete API to delete files | @Public |
| updateStatistics | `default UpdateStatistics updateStatistics()` | Create update statistics API | @Public |
| updatePartitionStatistics | `default UpdatePartitionStatistics updatePartitionStatistics()` | Create update partition statistics API | @Public |
| expireSnapshots | `ExpireSnapshots expireSnapshots()` | Create expire snapshots API | @Public |
| manageSnapshots | `ManageSnapshots manageSnapshots()` | Create manage snapshots API | @Public |
| newTransaction | `Transaction newTransaction()` | Create transaction API for multiple ops | @Public |
| io | `FileIO io()` | Returns FileIO for data/metadata files | @Public |
| encryption | `EncryptionManager encryption()` | Returns EncryptionManager | @Public |
| locationProvider | `LocationProvider locationProvider()` | Returns LocationProvider | @Public |
| statisticsFiles | `List<StatisticsFile> statisticsFiles()` | Returns current statistics files | @Public |
| partitionStatisticsFiles | `default List<PartitionStatisticsFile> partitionStatisticsFiles()` | Returns partition statistics files | @Public |
| refs | `Map<String, SnapshotRef> refs()` | Returns current refs for the table | @Public |
| uuid | `default UUID uuid()` | Returns the UUID of the table | @Public |

**Method Count**: 38

---

### 1.2 TableScan Interface (@Public)

**Description**: API for configuring a table scan.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| table | `Table table()` | Returns the Table from which this scan loads data | @Public |
| useSnapshot | `TableScan useSnapshot(long snapshotId)` | Use given snapshot by ID | @Public |
| useRef | `default TableScan useRef(String ref)` | Use given reference (branch/tag) | @Public |
| asOfTime | `TableScan asOfTime(long timestampMillis)` | Use snapshot as of given timestamp | @Public |
| appendsBetween | `default TableScan appendsBetween(long from, long to)` | Read appended data (deprecated) | @Deprecated |
| appendsAfter | `default TableScan appendsAfter(long fromSnapshotId)` | Read appended data after snapshot (deprecated) | @Deprecated |
| snapshot | `Snapshot snapshot()` | Returns the Snapshot that will be used | @Public |
| caseSensitive | `TableScan caseSensitive(boolean caseSensitive)` | Set case sensitivity for filtering | @Public (from Scan) |
| filter | `TableScan filter(Expression expr)` | Filter data using expression | @Public (from Scan) |
| filter | `Expression filter()` | Returns the filter expression | @Public (from Scan) |
| ignoreResiduals | `TableScan ignoreResiduals()` | Ignore residual filtering | @Public (from Scan) |
| includeColumnStats | `TableScan includeColumnStats()` | Include column statistics | @Public (from Scan) |
| includeColumnStats | `TableScan includeColumnStats(Collection<String> columns)` | Include stats for specific columns | @Public (from Scan) |
| isCaseSensitive | `boolean isCaseSensitive()` | Returns case sensitivity setting | @Public (from Scan) |
| metricsReporter | `TableScan metricsReporter(MetricsReporter reporter)` | Set metrics reporter | @Public (from Scan) |
| option | `TableScan option(String key, String value)` | Set scan option | @Public (from Scan) |
| planFiles | `CloseableIterable<FileScanTask> planFiles()` | Plan files to scan | @Public (from Scan) |
| planTasks | `CloseableIterable<CombinedScanTask> planTasks()` | Plan combined scan tasks | @Public (from Scan) |
| planWith | `TableScan planWith(ExecutorService executor)` | Use executor for planning | @Public (from Scan) |
| project | `TableScan project(Schema schema)` | Project specific schema | @Public (from Scan) |
| schema | `Schema schema()` | Returns the scan schema | @Public (from Scan) |
| select | `TableScan select(String... columns)` | Select specific columns | @Public (from Scan) |
| select | `TableScan select(Collection<String> columns)` | Select specific columns | @Public (from Scan) |
| splitLookback | `TableScan splitLookback(int lookback)` | Set split lookback | @Public (from Scan) |
| splitOpenFileCost | `TableScan splitOpenFileCost(long cost)` | Set split open file cost | @Public (from Scan) |
| targetSplitSize | `TableScan targetSplitSize(long size)` | Set target split size | @Public (from Scan) |

**Method Count**: 26 (including inherited from Scan)

---

### 1.3 Transaction Interface (@Public)

**Description**: A transaction for performing multiple updates to a table.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| table | `Table table()` | Return the Table that this transaction will update | @Public |
| updateSchema | `UpdateSchema updateSchema()` | Create UpdateSchema to alter columns | @Public |
| updateSpec | `UpdatePartitionSpec updateSpec()` | Create UpdatePartitionSpec | @Public |
| updateProperties | `UpdateProperties updateProperties()` | Create UpdateProperties | @Public |
| replaceSortOrder | `ReplaceSortOrder replaceSortOrder()` | Create ReplaceSortOrder | @Public |
| updateLocation | `UpdateLocation updateLocation()` | Create UpdateLocation | @Public |
| newAppend | `AppendFiles newAppend()` | Create append API | @Public |
| newFastAppend | `default AppendFiles newFastAppend()` | Create fast append API | @Public |
| newRewrite | `RewriteFiles newRewrite()` | Create rewrite API | @Public |
| rewriteManifests | `RewriteManifests rewriteManifests()` | Create rewrite manifests API | @Public |
| newOverwrite | `OverwriteFiles newOverwrite()` | Create overwrite API | @Public |
| newRowDelta | `RowDelta newRowDelta()` | Create row-level delta API | @Public |
| newReplacePartitions | `ReplacePartitions newReplacePartitions()` | Create replace partitions API | @Public |
| newDelete | `DeleteFiles newDelete()` | Create delete API | @Public |
| updateStatistics | `default UpdateStatistics updateStatistics()` | Create update statistics API | @Public |
| updatePartitionStatistics | `default UpdatePartitionStatistics updatePartitionStatistics()` | Create partition statistics API | @Public |
| expireSnapshots | `ExpireSnapshots expireSnapshots()` | Create expire snapshots API | @Public |
| manageSnapshots | `default ManageSnapshots manageSnapshots()` | Create manage snapshots API | @Public |
| commitTransaction | `void commitTransaction()` | Apply pending changes and commit | @Public |

**Method Count**: 19

---

### 1.4 UpdateSchema Interface (@Public)

**Description**: API for schema evolution.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| allowIncompatibleChanges | `UpdateSchema allowIncompatibleChanges()` | Allow incompatible changes | @Public |
| caseSensitive | `default UpdateSchema caseSensitive(boolean caseSensitive)` | Set case sensitivity | @Public |
| deleteColumn | `UpdateSchema deleteColumn(String name)` | Delete a column | @Public |
| makeColumnOptional | `UpdateSchema makeColumnOptional(String name)` | Make column optional | @Public |
| moveAfter | `UpdateSchema moveAfter(String name, String afterName)` | Move column after reference | @Public |
| moveBefore | `UpdateSchema moveBefore(String name, String beforeName)` | Move column before reference | @Public |
| moveFirst | `UpdateSchema moveFirst(String name)` | Move column to start | @Public |
| renameColumn | `UpdateSchema renameColumn(String name, String newName)` | Rename a column | @Public |
| requireColumn | `UpdateSchema requireColumn(String name)` | Make column required | @Public |
| setIdentifierFields | `UpdateSchema setIdentifierFields(String... names)` | Set identifier fields | @Public |
| setIdentifierFields | `UpdateSchema setIdentifierFields(Collection<String> names)` | Set identifier fields | @Public |
| unionByNameWith | `UpdateSchema unionByNameWith(Schema newSchema)` | Create union schema | @Public |
| updateColumn | `UpdateSchema updateColumn(String name, Type.PrimitiveType newType)` | Update column type | @Public |
| updateColumn | `default UpdateSchema updateColumn(String name, Type.PrimitiveType newType, String newDoc)` | Update column type and doc | @Public |
| updateColumnDefault | `default UpdateSchema updateColumnDefault(String name, Literal<?> newDefault)` | Update column default value | @Public |
| updateColumnDoc | `UpdateSchema updateColumnDoc(String name, String newDoc)` | Update column documentation | @Public |
| addColumn | `default UpdateSchema addColumn(String name, Type type)` | Add optional top-level column | @Public |
| addColumn | `default UpdateSchema addColumn(String name, Type type, String doc)` | Add column with doc | @Public |
| addColumn | `default UpdateSchema addColumn(String name, Type type, Literal<?> defaultValue)` | Add column with default | @Public |
| addColumn | `default UpdateSchema addColumn(String name, Type type, String doc, Literal<?> defaultValue)` | Add column with doc and default | @Public |
| addColumn | `default UpdateSchema addColumn(String parent, String name, Type type)` | Add column to nested struct | @Public |
| addColumn | `default UpdateSchema addColumn(String parent, String name, Type type, String doc)` | Add nested column with doc | @Public |
| addColumn | `default UpdateSchema addColumn(String parent, String name, Type type, Literal<?> defaultValue)` | Add nested column with default | @Public |
| addColumn | `default UpdateSchema addColumn(String parent, String name, Type type, String doc, Literal<?> defaultValue)` | Add nested column full options | @Public |
| addRequiredColumn | `default UpdateSchema addRequiredColumn(String name, Type type)` | Add required top-level column | @Public |
| addRequiredColumn | `default UpdateSchema addRequiredColumn(String name, Type type, String doc)` | Add required column with doc | @Public |
| addRequiredColumn | `default UpdateSchema addRequiredColumn(String name, Type type, Literal<?> defaultValue)` | Add required with default | @Public |
| addRequiredColumn | `default UpdateSchema addRequiredColumn(String name, Type type, String doc, Literal<?> defaultValue)` | Add required full options | @Public |
| addRequiredColumn | `default UpdateSchema addRequiredColumn(String parent, String name, Type type)` | Add required to nested | @Public |
| addRequiredColumn | `default UpdateSchema addRequiredColumn(String parent, String name, Type type, String doc)` | Add required nested with doc | @Public |
| addRequiredColumn | `default UpdateSchema addRequiredColumn(String parent, String name, Type type, Literal<?> defaultValue)` | Add required nested with default | @Public |
| addRequiredColumn | `default UpdateSchema addRequiredColumn(String parent, String name, Type type, String doc, Literal<?> defaultValue)` | Add required nested full options | @Public |
| apply | `Schema apply()` | Apply changes and return new schema | @Public (from PendingUpdate) |
| commit | `void commit()` | Commit the changes | @Public (from PendingUpdate) |

**Method Count**: 33 (including inherited)

---

### 1.5 AppendFiles Interface (@Public)

**Description**: API for appending new files in a table.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| appendFile | `AppendFiles appendFile(DataFile file)` | Append a DataFile to the table | @Public |
| appendManifest | `AppendFiles appendManifest(ManifestFile file)` | Append a ManifestFile to the table | @Public |
| deleteWith | `AppendFiles deleteWith(Consumer<String> deleteFunc)` | Set delete callback | @Public (from SnapshotUpdate) |
| scanManifestsWith | `AppendFiles scanManifestsWith(ExecutorService executor)` | Use executor for scanning | @Public (from SnapshotUpdate) |
| set | `AppendFiles set(String key, String value)` | Set snapshot property | @Public (from SnapshotUpdate) |
| stageOnly | `AppendFiles stageOnly()` | Stage changes without committing | @Public (from SnapshotUpdate) |
| toBranch | `AppendFiles toBranch(String branch)` | Commit to specific branch | @Public (from SnapshotUpdate) |
| apply | `Snapshot apply()` | Apply and return snapshot | @Public (from PendingUpdate) |
| commit | `void commit()` | Commit the changes | @Public (from PendingUpdate) |

**Method Count**: 9

---

### 1.6 DeleteFiles Interface (@Public)

**Description**: API for deleting files from a table.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| deleteFile | `DeleteFiles deleteFile(CharSequence path)` | Delete a file path from the table | @Public |
| deleteFile | `default DeleteFiles deleteFile(DataFile file)` | Delete a DataFile from the table | @Public |
| deleteFromRowFilter | `DeleteFiles deleteFromRowFilter(Expression expr)` | Delete files matching expression | @Public |
| caseSensitive | `DeleteFiles caseSensitive(boolean caseSensitive)` | Set case sensitivity for binding | @Public |
| validateFilesExist | `default DeleteFiles validateFilesExist()` | Validate files exist at commit | @Public |
| deleteWith | `DeleteFiles deleteWith(Consumer<String> deleteFunc)` | Set delete callback | @Public (from SnapshotUpdate) |
| scanManifestsWith | `DeleteFiles scanManifestsWith(ExecutorService executor)` | Use executor for scanning | @Public (from SnapshotUpdate) |
| set | `DeleteFiles set(String key, String value)` | Set snapshot property | @Public (from SnapshotUpdate) |
| stageOnly | `DeleteFiles stageOnly()` | Stage changes without committing | @Public (from SnapshotUpdate) |
| toBranch | `DeleteFiles toBranch(String branch)` | Commit to specific branch | @Public (from SnapshotUpdate) |
| apply | `Snapshot apply()` | Apply and return snapshot | @Public (from PendingUpdate) |
| commit | `void commit()` | Commit the changes | @Public (from PendingUpdate) |

**Method Count**: 12

---

### 1.7 OverwriteFiles Interface (@Public)

**Description**: API for overwriting files in a table by a filter expression.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| overwriteByRowFilter | `OverwriteFiles overwriteByRowFilter(Expression expr)` | Overwrite files matching expression | @Public |
| addFile | `OverwriteFiles addFile(DataFile file)` | Add a file to be added in overwrite | @Public |
| caseSensitive | `OverwriteFiles caseSensitive(boolean caseSensitive)` | Set case sensitivity | @Public |
| validateAddedFilesMatchOverwriteFilter | `default OverwriteFiles validateAddedFilesMatchOverwriteFilter()` | Validate added files | @Public |
| validateFromSnapshot | `OverwriteFiles validateFromSnapshot(long snapshotId)` | Validate base snapshot | @Public |
| deleteWith | `OverwriteFiles deleteWith(Consumer<String> deleteFunc)` | Set delete callback | @Public (from SnapshotUpdate) |
| scanManifestsWith | `OverwriteFiles scanManifestsWith(ExecutorService executor)` | Use executor for scanning | @Public (from SnapshotUpdate) |
| set | `OverwriteFiles set(String key, String value)` | Set snapshot property | @Public (from SnapshotUpdate) |
| stageOnly | `OverwriteFiles stageOnly()` | Stage changes | @Public (from SnapshotUpdate) |
| toBranch | `OverwriteFiles toBranch(String branch)` | Commit to branch | @Public (from SnapshotUpdate) |
| apply | `Snapshot apply()` | Apply and return snapshot | @Public (from PendingUpdate) |
| commit | `void commit()` | Commit the changes | @Public (from PendingUpdate) |

**Method Count**: 12

---

### 1.8 RewriteFiles Interface (@Public)

**Description**: API for rewriting files in a table.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| rewriteFiles | `RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd)` | Rewrite files | @Public |
| rewriteFiles | `RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DeleteFile> deleteFilesToDelete, Set<DataFile> filesToAdd)` | Rewrite with deletes | @Public |
| rewriteFiles | `RewriteFiles rewriteFiles(Set<DataFile> dataFilesToDelete, Set<DeleteFile> deleteFilesToDelete, Set<DataFile> dataFilesToAdd, Set<DeleteFile> deleteFilesToAdd)` | Full rewrite | @Public |
| validateFromSnapshot | `RewriteFiles validateFromSnapshot(long snapshotId)` | Validate base snapshot | @Public |
| dataSequenceNumber | `RewriteFiles dataSequenceNumber(long seqNum)` | Set data sequence number | @Public |
| deleteWith | `RewriteFiles deleteWith(Consumer<String> deleteFunc)` | Set delete callback | @Public (from SnapshotUpdate) |
| scanManifestsWith | `RewriteFiles scanManifestsWith(ExecutorService executor)` | Use executor | @Public (from SnapshotUpdate) |
| set | `RewriteFiles set(String key, String value)` | Set snapshot property | @Public (from SnapshotUpdate) |
| stageOnly | `RewriteFiles stageOnly()` | Stage changes | @Public (from SnapshotUpdate) |
| toBranch | `RewriteFiles toBranch(String branch)` | Commit to branch | @Public (from SnapshotUpdate) |
| apply | `Snapshot apply()` | Apply and return snapshot | @Public (from PendingUpdate) |
| commit | `void commit()` | Commit the changes | @Public (from PendingUpdate) |

**Method Count**: 12

---

### 1.9 RowDelta Interface (@Public)

**Description**: API for row-level delta operations (position deletes and equality deletes).

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| addDeletes | `RowDelta addDeletes(DeleteFile deleteFile)` | Add delete file | @Public |
| addRows | `RowDelta addRows(DataFile dataFile)` | Add data file | @Public |
| validateFromSnapshot | `RowDelta validateFromSnapshot(long snapshotId)` | Validate base snapshot | @Public |
| validateDataFilesExist | `RowDelta validateDataFilesExist()` | Validate data files exist | @Public |
| caseSensitive | `RowDelta caseSensitive(boolean caseSensitive)` | Set case sensitivity | @Public |
| deleteWith | `RowDelta deleteWith(Consumer<String> deleteFunc)` | Set delete callback | @Public (from SnapshotUpdate) |
| scanManifestsWith | `RowDelta scanManifestsWith(ExecutorService executor)` | Use executor | @Public (from SnapshotUpdate) |
| set | `RowDelta set(String key, String value)` | Set snapshot property | @Public (from SnapshotUpdate) |
| stageOnly | `RowDelta stageOnly()` | Stage changes | @Public (from SnapshotUpdate) |
| toBranch | `RowDelta toBranch(String branch)` | Commit to branch | @Public (from SnapshotUpdate) |
| apply | `Snapshot apply()` | Apply and return snapshot | @Public (from PendingUpdate) |
| commit | `void commit()` | Commit the changes | @Public (from PendingUpdate) |

**Method Count**: 12

---

### 1.10 ExpireSnapshots Interface (@Public)

**Description**: API for expiring snapshots from a table.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| expireSnapshotId | `ExpireSnapshots expireSnapshotId(long snapshotId)` | Expire specific snapshot | @Public |
| expireSnapshotId | `ExpireSnapshots expireSnapshotId(long snapshotId, String branch)` | Expire snapshot on branch | @Public |
| expireOlderThan | `ExpireSnapshots expireOlderThan(long timestampMillis)` | Expire snapshots older than timestamp | @Public |
| retainLast | `ExpireSnapshots retainLast(int numSnapshots)` | Retain last N snapshots | @Public |
| retainLast | `ExpireSnapshots retainLast(int numSnapshots, String branch)` | Retain last N on branch | @Public |
| retainSnapshotsWithIds | `ExpireSnapshots retainSnapshotsWithIds(Set<Long> ids)` | Retain specific snapshots | @Public |
| cleanExpiredFiles | `default ExpireSnapshots cleanExpiredFiles()` | Clean expired files | @Public |
| deleteWith | `ExpireSnapshots deleteWith(Consumer<String> deleteFunc)` | Set delete callback | @Public |
| executeDeleteWith | `ExpireSnapshots executeDeleteWith(ExecutorService executor)` | Use executor for deletes | @Public |
| planWith | `ExpireSnapshots planWith(ExecutorService executor)` | Use executor for planning | @Public |
| apply | `List<Snapshot> apply()` | Apply and return expired snapshots | @Public (from PendingUpdate) |
| commit | `void commit()` | Commit the changes | @Public (from PendingUpdate) |

**Method Count**: 12

---

### 1.11 ManageSnapshots Interface (@Public)

**Description**: API for managing table snapshots (branches and tags).

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| createBranch | `ManageSnapshots createBranch(String name, long snapshotId)` | Create a branch | @Public |
| createBranch | `ManageSnapshots createBranch(String name, String sourceRef)` | Create branch from ref | @Public |
| createTag | `ManageSnapshots createTag(String name, long snapshotId)` | Create a tag | @Public |
| createTag | `ManageSnapshots createTag(String name, String sourceRef)` | Create tag from ref | @Public |
| replaceBranch | `ManageSnapshots replaceBranch(String name, long snapshotId)` | Replace branch snapshot | @Public |
| replaceBranch | `ManageSnapshots replaceBranch(String name, String sourceRef)` | Replace branch from ref | @Public |
| replaceTag | `ManageSnapshots replaceTag(String name, long snapshotId)` | Replace tag snapshot | @Public |
| replaceTag | `ManageSnapshots replaceTag(String name, String sourceRef)` | Replace tag from ref | @Public |
| renameBranch | `ManageSnapshots renameBranch(String name, String newName)` | Rename a branch | @Public |
| removeBranch | `ManageSnapshots removeBranch(String name)` | Remove a branch | @Public |
| removeTag | `ManageSnapshots removeTag(String name)` | Remove a tag | @Public |
| setMinSnapshotsToKeep | `ManageSnapshots setMinSnapshotsToKeep(String name, int minSnapshots)` | Set min snapshots | @Public |
| setMaxSnapshotAgeMs | `ManageSnapshots setMaxSnapshotAgeMs(String name, long maxAgeMs)` | Set max snapshot age | @Public |
| setMaxRefAgeMs | `ManageSnapshots setMaxRefAgeMs(String name, long maxAgeMs)` | Set max ref age | @Public |
| fastForward | `ManageSnapshots fastForward(String branch, String sourceRef)` | Fast-forward branch | @Public |
| apply | `Snapshot apply()` | Apply and return current snapshot | @Public (from PendingUpdate) |
| commit | `void commit()` | Commit the changes | @Public (from PendingUpdate) |

**Method Count**: 17

---

### 1.12 DataFile Interface (@Public)

**Description**: Interface for data files listed in a table manifest.

**Fields**:
| Field | Type | Description |
|-------|------|-------------|
| CONTENT | NestedField | File content type |
| FILE_PATH | NestedField | File path |
| FILE_FORMAT | NestedField | File format (AVRO/PARQUET/ORC) |
| RECORD_COUNT | NestedField | Number of records |
| FILE_SIZE | NestedField | File size in bytes |
| COLUMN_SIZES | NestedField | Column size map |
| VALUE_COUNTS | NestedField | Value count map |
| NULL_VALUE_COUNTS | NestedField | Null value count map |
| NAN_VALUE_COUNTS | NestedField | NaN value count map |
| LOWER_BOUNDS | NestedField | Lower bounds map |
| UPPER_BOUNDS | NestedField | Upper bounds map |
| KEY_METADATA | NestedField | Encryption key metadata |
| SPLIT_OFFSETS | NestedField | Split offsets |
| EQUALITY_IDS | NestedField | Equality field IDs |
| SORT_ORDER_ID | NestedField | Sort order ID |
| SPEC_ID | NestedField | Partition spec ID |
| FIRST_ROW_ID | NestedField | First row ID |
| REFERENCED_DATA_FILE | NestedField | Referenced data file path |
| CONTENT_OFFSET | NestedField | Content offset |
| CONTENT_SIZE | NestedField | Content size |

**Methods (from ContentFile)**:
| Method | Signature | Description |
|--------|-----------|-------------|
| content | `default FileContent content()` | Returns file content type (DATA/POSITION_DELETES/EQUALITY_DELETES) |
| equalityFieldIds | `default List<Integer> equalityFieldIds()` | Returns equality field IDs for delete files |
| path | `String path()` | Returns full file path |
| format | `FileFormat format()` | Returns file format |
| specId | `int specId()` | Returns partition spec ID |
| partition | `StructLike partition()` | Returns partition data |
| recordCount | `long recordCount()` | Returns record count |
| fileSizeInBytes | `long fileSizeInBytes()` | Returns file size |
| columnSizes | `Map<Integer, Long> columnSizes()` | Returns column sizes |
| valueCounts | `Map<Integer, Long> valueCounts()` | Returns value counts |
| nullValueCounts | `Map<Integer, Long> nullValueCounts()` | Returns null counts |
| nanValueCounts | `Map<Integer, Long> nanValueCounts()` | Returns NaN counts |
| lowerBounds | `Map<Integer, ByteBuffer> lowerBounds()` | Returns lower bounds |
| upperBounds | `Map<Integer, ByteBuffer> upperBounds()` | Returns upper bounds |
| keyMetadata | `ByteBuffer keyMetadata()` | Returns key metadata |
| splitOffsets | `List<Long> splitOffsets()` | Returns split offsets |
| sortOrderId | `int sortOrderId()` | Returns sort order ID |
| copy | `DataFile copy()` | Returns a copy |
| copy | `DataFile copy(boolean withStats)` | Copy with or without stats |
| copyWithoutStats | `DataFile copyWithoutStats()` | Copy without stats |
| copyWithStats | `DataFile copyWithStats(Set<Integer> requestedStats)` | Copy with specific stats |
| dataSequenceNumber | `Long dataSequenceNumber()` | Returns data sequence number |
| fileSequenceNumber | `Long fileSequenceNumber()` | Returns file sequence number |
| firstRowId | `Long firstRowId()` | Returns first row ID |
| manifestLocation | `String manifestLocation()` | Returns manifest location |
| pos | `Long pos()` | Returns position in manifest |

**Method Count**: 28 (including inherited from ContentFile)

---

### 1.13 DeleteFile Interface (@Public)

**Description**: Interface for delete files listed in a table manifest.

**Methods**: Inherits from ContentFile<DeleteFile>, same methods as DataFile.

**Method Count**: 28

---

### 1.14 FileScanTask Interface (@Public)

**Description**: A scan task over a range of bytes in a single data file.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| deletes | `List<DeleteFile> deletes()` | List of delete files to apply | @Public |
| schema | `default Schema schema()` | Return schema for this scan task | @Public |
| sizeBytes | `default long sizeBytes()` | Number of bytes to read | @Public |
| filesCount | `default int filesCount()` | Number of files to open | @Public |
| isFileScanTask | `default boolean isFileScanTask()` | Check if this is FileScanTask | @Public |
| asFileScanTask | `default FileScanTask asFileScanTask()` | Cast to FileScanTask | @Public |
| file | `DataFile file()` | Returns the data file (from ContentScanTask) | @Public |
| start | `long start()` | Returns start offset (from ContentScanTask) | @Public |
| length | `long length()` | Returns length (from ContentScanTask) | @Public |
| partition | `StructLike partition()` | Returns partition (from PartitionScanTask) | @Public |
| spec | `PartitionSpec spec()` | Returns partition spec (from PartitionScanTask) | @Public |
| residual | `Expression residual()` | Returns residual filter (from ContentScanTask) | @Public |
| estimatedRowsCount | `long estimatedRowsCount()` | Estimated row count | @Public |
| split | `Iterable<FileScanTask> split(long targetSize)` | Split task (from SplittableScanTask) | @Public |

**Method Count**: 14

---

### 1.15 Snapshot Interface (@Public)

**Description**: A snapshot of an Iceberg table at a point in time.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| snapshotId | `long snapshotId()` | Returns the snapshot ID | @Public |
| timestampMillis | `long timestampMillis()` | Returns timestamp in milliseconds | @Public |
| operation | `String operation()` | Returns operation type (append/overwrite/delete) | @Public |
| summary | `Map<String, String> summary()` | Returns summary map | @Public |
| manifestListLocation | `String manifestListLocation()` | Returns manifest list location | @Public |
| manifests | `List<ManifestFile> manifests()` | Returns manifests | @Public |
| allManifests | `List<ManifestFile> allManifests(FileIO io)` | Returns all manifests | @Public |
| addedFiles | `List<DataFile> addedFiles(FileIO io)` | Returns added data files | @Public |
| addedDataFiles | `Iterable<DataFile> addedDataFiles(FileIO io)` | Returns added data files | @Public |
| deletedFiles | `List<DataFile> deletedFiles(FileIO io)` | Returns deleted data files | @Public |
| deletedDataFiles | `Iterable<DataFile> deletedDataFiles(FileIO io)` | Returns deleted data files | @Public |
| partitionDataFiles | `Iterable<DataFile> partitionDataFiles(FileIO io, PartitionSpec spec, StructLike partition)` | Files for partition | @Public |
| schemaId | `Integer schemaId()` | Returns schema ID | @Public |
| manifestLocations | `Iterable<String> manifestLocations()` | Returns manifest locations | @Public |
| parentId | `Long parentId()` | Returns parent snapshot ID | @Public |
| sequenceNumber | `long sequenceNumber()` | Returns sequence number | @Public |
| addedDataSequenceNumber | `Long addedDataSequenceNumber()` | Returns added data sequence number | @Public |
| firstRowId | `Long firstRowId()` | Returns first row ID | @Public |

**Method Count**: 18

---

### 1.16 PartitionStatisticsFile Interface (@Public)

**Description**: Represents a partition statistics file for efficient data reading.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| snapshotId | `long snapshotId()` | ID of snapshot associated with statistics | @Public |
| path | `String path()` | Fully qualified path to the file | @Public |
| fileSizeInBytes | `long fileSizeInBytes()` | Size of the file in bytes | @Public |

**Method Count**: 3

---

## 2. Catalog API (org.apache.iceberg.catalog)

### 2.1 Catalog Interface (@Public)

**Description**: A Catalog API for table create, drop, and load operations.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| name | `default String name()` | Return the name for this catalog | @Public |
| listTables | `List<TableIdentifier> listTables(Namespace namespace)` | Return all identifiers under namespace | @Public |
| createTable | `default Table createTable(TableIdentifier identifier, Schema schema)` | Create unpartitioned table | @Public |
| createTable | `default Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec)` | Create table with spec | @Public |
| createTable | `default Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties)` | Create table with properties | @Public |
| createTable | `default Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties)` | Create table with location | @Public |
| newCreateTableTransaction | `default Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema)` | Start create table transaction | @Public |
| newCreateTableTransaction | `default Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec)` | Start transaction with spec | @Public |
| newCreateTableTransaction | `default Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties)` | Transaction with properties | @Public |
| newCreateTableTransaction | `default Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties)` | Transaction full options | @Public |
| newReplaceTableTransaction | `default Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, boolean orCreate)` | Start replace table transaction | @Public |
| newReplaceTableTransaction | `default Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, boolean orCreate)` | Replace with spec | @Public |
| newReplaceTableTransaction | `default Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties, boolean orCreate)` | Replace with properties | @Public |
| newReplaceTableTransaction | `default Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties, boolean orCreate)` | Replace full options | @Public |
| tableExists | `default boolean tableExists(TableIdentifier identifier)` | Check if table exists | @Public |
| dropTable | `default boolean dropTable(TableIdentifier identifier)` | Drop table and delete files | @Public |
| dropTable | `boolean dropTable(TableIdentifier identifier, boolean purge)` | Drop table optionally deleting files | @Public |
| renameTable | `void renameTable(TableIdentifier from, TableIdentifier to)` | Rename a table | @Public |
| loadTable | `Table loadTable(TableIdentifier identifier)` | Load a table | @Public |
| invalidateTable | `default void invalidateTable(TableIdentifier identifier)` | Invalidate cached table metadata | @Public |
| registerTable | `default Table registerTable(TableIdentifier identifier, String metadataFileLocation)` | Register existing table | @Public |
| buildTable | `default Catalog.TableBuilder buildTable(TableIdentifier identifier, Schema schema)` | Instantiate a table builder | @Public |
| initialize | `default void initialize(String name, Map<String, String> properties)` | Initialize catalog | @Public |

**Method Count**: 23

---

### 2.2 Catalog.TableBuilder Interface (@Public)

**Description**: A builder to create tables or start create/replace transactions.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| withPartitionSpec | `TableBuilder withPartitionSpec(PartitionSpec spec)` | Set partition spec | @Public |
| withSortOrder | `TableBuilder withSortOrder(SortOrder sortOrder)` | Set sort order | @Public |
| withLocation | `TableBuilder withLocation(String location)` | Set location | @Public |
| withProperties | `TableBuilder withProperties(Map<String, String> properties)` | Set properties | @Public |
| create | `Table create()` | Create the table | @Public |
| createTransaction | `Transaction createTransaction()` | Start create transaction | @Public |
| replaceTransaction | `Transaction replaceTransaction()` | Start replace transaction | @Public |
| createOrReplaceTransaction | `Transaction createOrReplaceTransaction()` | Start create-or-replace transaction | @Public |

**Method Count**: 8

---

### 2.3 TableIdentifier Class (@Public)

**Description**: Identifies a table in Iceberg catalog.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| of | `static TableIdentifier of(String... names)` | Create identifier from names | @Public |
| of | `static TableIdentifier of(Namespace namespace, String name)` | Create identifier from namespace and name | @Public |
| parse | `static TableIdentifier parse(String identifier)` | Parse identifier string | @Public |
| hasNamespace | `boolean hasNamespace()` | Whether namespace is not empty | @Public |
| namespace | `Namespace namespace()` | Returns the identifier namespace | @Public |
| name | `String name()` | Returns the identifier name | @Public |
| toLowerCase | `TableIdentifier toLowerCase()` | Convert to lowercase | @Public |
| equals | `boolean equals(Object other)` | Check equality | @Public |
| hashCode | `int hashCode()` | Returns hash code | @Public |
| toString | `String toString()` | Returns string representation | @Public |

**Method Count**: 10

---

### 2.4 Namespace Class (@Public)

**Description**: A namespace in a Catalog.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| empty | `static Namespace empty()` | Create empty namespace | @Public |
| of | `static Namespace of(String... levels)` | Create namespace from levels | @Public |
| levels | `String[] levels()` | Returns namespace levels | @Public |
| level | `String level(int pos)` | Returns level at position | @Public |
| isEmpty | `boolean isEmpty()` | Check if namespace is empty | @Public |
| length | `int length()` | Returns number of levels | @Public |
| equals | `boolean equals(Object other)` | Check equality | @Public |
| hashCode | `int hashCode()` | Returns hash code | @Public |
| toString | `String toString()` | Returns string representation | @Public |

**Method Count**: 9

---

## 3. Types API (org.apache.iceberg.types)

### 3.1 Type Interface (@Public)

**Description**: Base interface for all Iceberg types.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| typeId | `Type.TypeID typeId()` | Returns the type ID | @Public |
| isPrimitiveType | `default boolean isPrimitiveType()` | Check if primitive type | @Public |
| asPrimitiveType | `default Type.PrimitiveType asPrimitiveType()` | Cast to primitive type | @Public |
| asStructType | `default Types.StructType asStructType()` | Cast to struct type | @Public |
| asListType | `default Types.ListType asListType()` | Cast to list type | @Public |
| asMapType | `default Types.MapType asMapType()` | Cast to map type | @Public |
| asVariantType | `default Types.VariantType asVariantType()` | Cast to variant type | @Public |
| isNestedType | `default boolean isNestedType()` | Check if nested type | @Public |
| isStructType | `default boolean isStructType()` | Check if struct type | @Public |
| isListType | `default boolean isListType()` | Check if list type | @Public |
| isMapType | `default boolean isMapType()` | Check if map type | @Public |
| isVariantType | `default boolean isVariantType()` | Check if variant type | @Public |
| asNestedType | `default Type.NestedType asNestedType()` | Cast to nested type | @Public |

**Method Count**: 13

---

### 3.2 Types Class (@Public)

**Description**: Factory class and nested types for Iceberg data types.

**Nested Classes**:

| Class | Description |
|-------|-------------|
| Types.BooleanType | Boolean type |
| Types.IntegerType | 32-bit signed integer |
| Types.LongType | 64-bit signed integer |
| Types.FloatType | 32-bit IEEE 754 floating point |
| Types.DoubleType | 64-bit IEEE 754 floating point |
| Types.DecimalType | Fixed-point decimal with precision and scale |
| Types.StringType | UTF-8 encoded string |
| Types.UUIDType | Universally Unique Identifier |
| Types.FixedType | Fixed-length byte array |
| Types.BinaryType | Arbitrary-length byte array |
| Types.DateType | Date without timezone (days from epoch) |
| Types.TimeType | Time without timezone (microseconds from midnight) |
| Types.TimestampType | Timestamp with timezone (microseconds from epoch) |
| Types.TimestampNanoType | Timestamp with timezone (nanoseconds from epoch) |
| Types.GeographyType | Geography type (GeoJSON) |
| Types.GeometryType | Geometry type (GeoJSON) |
| Types.UnknownType | Unknown type |
| Types.VariantType | Variant type (semi-structured data) |
| Types.StructType | Struct type (tuple of fields) |
| Types.ListType | List type (array of elements) |
| Types.MapType | Map type (key-value pairs) |
| Types.NestedField | Nested field with ID, name, type, and doc |

**Static Methods**:

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| fromTypeName | `static Type fromTypeName(String typeString)` | Parse type from string name | @Public |
| fromPrimitiveString | `static Type.PrimitiveType fromPrimitiveString(String typeString)` | Parse primitive type from string | @Public |

**NestedField Methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| fieldId | `int fieldId()` | Returns field ID |
| name | `String name()` | Returns field name |
| type | `Type type()` | Returns field type |
| doc | `String doc()` | Returns field documentation |
| isRequired | `boolean isRequired()` | Check if required field |
| isOptional | `boolean isOptional()` | Check if optional field |
| initialDefault | `Object initialDefault()` | Returns initial default value |
| writeDefault | `Object writeDefault()` | Returns write default value |

**StructType Methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| fields | `List<NestedField> fields()` | Returns struct fields |
| field | `NestedField field(int id)` | Returns field by ID |
| field | `NestedField field(String name)` | Returns field by name |
| fieldsByName | `Map<String, NestedField> fieldsByName()` | Returns fields by name map |

**ListType Methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| fields | `List<NestedField> fields()` | Returns list fields |
| elementId | `int elementId()` | Returns element field ID |
| elementType | `Type elementType()` | Returns element type |
| isElementRequired | `boolean isElementRequired()` | Check if element required |
| isElementOptional | `boolean isElementOptional()` | Check if element optional |

**MapType Methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| fields | `List<NestedField> fields()` | Returns map fields |
| keyId | `int keyId()` | Returns key field ID |
| keyType | `Type keyType()` | Returns key type |
| valueId | `int valueId()` | Returns value field ID |
| valueType | `Type valueType()` | Returns value type |
| isValueRequired | `boolean isValueRequired()` | Check if value required |
| isValueOptional | `boolean isValueOptional()` | Check if value optional |

**Total Method Count**: ~50 (including nested class methods)

---

### 3.3 Type.TypeID Enum (@Public)

**Values**: BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL, STRING, UUID, FIXED, BINARY, DATE, TIME, TIMESTAMP, TIMESTAMP_NANO, GEOGRAPHY, GEOMETRY, UNKNOWN, VARIANT, LIST, MAP, STRUCT

---

## 4. Expressions API (org.apache.iceberg.expressions)

### 4.1 Expressions Class (@Public)

**Description**: Factory methods for creating expressions.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| alwaysTrue | `static True alwaysTrue()` | Returns always-true expression | @Public |
| alwaysFalse | `static False alwaysFalse()` | Returns always-false expression | @Public |
| and | `static Expression and(Expression left, Expression right)` | And expression | @Public |
| and | `static Expression and(Expression left, Expression right, Expression... exprs)` | And multiple expressions | @Public |
| or | `static Expression or(Expression left, Expression right)` | Or expression | @Public |
| not | `static Expression not(Expression child)` | Not expression | @Public |
| isNull | `static UnboundPredicate<T> isNull(String name)` | Is null predicate | @Public |
| isNull | `static UnboundPredicate<T> isNull(UnboundTerm<T> expr)` | Is null predicate | @Public |
| notNull | `static UnboundPredicate<T> notNull(String name)` | Not null predicate | @Public |
| notNull | `static UnboundPredicate<T> notNull(UnboundTerm<T> expr)` | Not null predicate | @Public |
| isNaN | `static UnboundPredicate<T> isNaN(String name)` | Is NaN predicate | @Public |
| isNaN | `static UnboundPredicate<T> isNaN(UnboundTerm<T> expr)` | Is NaN predicate | @Public |
| notNaN | `static UnboundPredicate<T> notNaN(String name)` | Not NaN predicate | @Public |
| notNaN | `static UnboundPredicate<T> notNaN(UnboundTerm<T> expr)` | Not NaN predicate | @Public |
| lessThan | `static UnboundPredicate<T> lessThan(String name, T value)` | Less than predicate | @Public |
| lessThan | `static UnboundPredicate<T> lessThan(UnboundTerm<T> expr, T value)` | Less than predicate | @Public |
| lessThanOrEqual | `static UnboundPredicate<T> lessThanOrEqual(String name, T value)` | Less than or equal | @Public |
| lessThanOrEqual | `static UnboundPredicate<T> lessThanOrEqual(UnboundTerm<T> expr, T value)` | Less than or equal | @Public |
| greaterThan | `static UnboundPredicate<T> greaterThan(String name, T value)` | Greater than predicate | @Public |
| greaterThan | `static UnboundPredicate<T> greaterThan(UnboundTerm<T> expr, T value)` | Greater than predicate | @Public |
| greaterThanOrEqual | `static UnboundPredicate<T> greaterThanOrEqual(String name, T value)` | Greater than or equal | @Public |
| greaterThanOrEqual | `static UnboundPredicate<T> greaterThanOrEqual(UnboundTerm<T> expr, T value)` | Greater than or equal | @Public |
| equal | `static UnboundPredicate<T> equal(String name, T value)` | Equal predicate | @Public |
| equal | `static UnboundPredicate<T> equal(UnboundTerm<T> expr, T value)` | Equal predicate | @Public |
| notEqual | `static UnboundPredicate<T> notEqual(String name, T value)` | Not equal predicate | @Public |
| notEqual | `static UnboundPredicate<T> notEqual(UnboundTerm<T> expr, T value)` | Not equal predicate | @Public |
| startsWith | `static UnboundPredicate<String> startsWith(String name, String value)` | Starts with predicate | @Public |
| startsWith | `static UnboundPredicate<String> startsWith(UnboundTerm<String> expr, String value)` | Starts with predicate | @Public |
| notStartsWith | `static UnboundPredicate<String> notStartsWith(String name, String value)` | Not starts with | @Public |
| notStartsWith | `static UnboundPredicate<String> notStartsWith(UnboundTerm<String> expr, String value)` | Not starts with | @Public |
| in | `static UnboundPredicate<T> in(String name, T... values)` | In predicate | @Public |
| in | `static UnboundPredicate<T> in(UnboundTerm<T> expr, T... values)` | In predicate | @Public |
| in | `static UnboundPredicate<T> in(String name, Iterable<T> values)` | In predicate | @Public |
| in | `static UnboundPredicate<T> in(UnboundTerm<T> expr, Iterable<T> values)` | In predicate | @Public |
| notIn | `static UnboundPredicate<T> notIn(String name, T... values)` | Not in predicate | @Public |
| notIn | `static UnboundPredicate<T> notIn(UnboundTerm<T> expr, T... values)` | Not in predicate | @Public |
| notIn | `static UnboundPredicate<T> notIn(String name, Iterable<T> values)` | Not in predicate | @Public |
| notIn | `static UnboundPredicate<T> notIn(UnboundTerm<T> expr, Iterable<T> values)` | Not in predicate | @Public |
| bucket | `static UnboundTerm<T> bucket(String name, int numBuckets)` | Bucket transform | @Public |
| year | `static UnboundTerm<T> year(String name)` | Year transform | @Public |
| month | `static UnboundTerm<T> month(String name)` | Month transform | @Public |
| day | `static UnboundTerm<T> day(String name)` | Day transform | @Public |
| hour | `static UnboundTerm<T> hour(String name)` | Hour transform | @Public |
| truncate | `static UnboundTerm<T> truncate(String name, int width)` | Truncate transform | @Public |
| extract | `static UnboundTerm<T> extract(String name, String path, String type)` | Extract transform for Variant | @Public |
| ref | `static NamedReference<T> ref(String name)` | Create column reference | @Public |
| transform | `static UnboundTerm<T> transform(String name, Transform<?, T> transform)` | Create transform expression | @Public |
| lit | `static Literal<T> lit(T value)` | Create literal from object | @Public |
| micros | `static Literal<Long> micros(long micros)` | Create timestamp literal (micros) | @Public |
| millis | `static Literal<Long> millis(long millis)` | Create timestamp literal (millis) | @Public |
| nanos | `static Literal<Long> nanos(long nanos)` | Create timestamp literal (nanos) | @Public |
| count | `static UnboundAggregate<T> count(String name)` | Count aggregate | @Public |
| countStar | `static UnboundAggregate<T> countStar()` | Count star aggregate | @Public |
| max | `static UnboundAggregate<T> max(String name)` | Max aggregate | @Public |
| min | `static UnboundAggregate<T> min(String name)` | Min aggregate | @Public |
| predicate | `static UnboundPredicate<T> predicate(Operation op, String name)` | Create predicate by operation | @Public |
| predicate | `static UnboundPredicate<T> predicate(Operation op, String name, T value)` | Create predicate with value | @Public |
| predicate | `static UnboundPredicate<T> predicate(Operation op, String name, Literal<T> lit)` | Create predicate with literal | @Public |
| predicate | `static UnboundPredicate<T> predicate(Operation op, String name, Iterable<T> values)` | Create predicate with values | @Public |
| predicate | `static UnboundPredicate<T> predicate(Operation op, UnboundTerm<T> expr)` | Create predicate on term | @Public |
| predicate | `static UnboundPredicate<T> predicate(Operation op, UnboundTerm<T> expr, Iterable<T> values)` | Create predicate on term | @Public |
| rewriteNot | `static Expression rewriteNot(Expression expr)` | Rewrite not expressions | @Public |

**Method Count**: 62

---

### 4.2 Expression Interface (@Public)

**Description**: Base interface for all expressions.

**Operation Enum**: IS_NULL, NOT_NULL, IS_NAN, NOT_NAN, LT, LT_EQ, GT, GT_EQ, EQ, NOT_EQ, STARTS_WITH, NOT_STARTS_WITH, IN, NOT_IN, AND, OR, NOT

---

### 4.3 UnboundPredicate Class (@Public)

**Description**: An unbound predicate before binding to a schema.

| Method | Signature | Description |
|--------|-----------|-------------|
| op | `Expression.Operation op()` | Returns the operation |
| term | `UnboundTerm<T> term()` | Returns the term |
| literals | `List<Literal<T>> literals()` | Returns literals |

---

### 4.4 Literal Interface (@Public)

**Description**: A literal value in an expression.

| Method | Signature | Description |
|--------|-----------|-------------|
| value | `T value()` | Returns the value |
| to | `Literal<U> to(Type type)` | Convert to another type |

---

## 5. View API (org.apache.iceberg.view)

### 5.1 View Interface (@Public)

**Description**: Interface for view definition.

| Method | Signature | Description | Stability |
|--------|-----------|-------------|-----------|
| name | `String name()` | Returns the view name | @Public |
| schema | `Schema schema()` | Return the schema for this view | @Public |
| schemas | `Map<Integer, Schema> schemas()` | Return a map of schemas | @Public |
| currentVersion | `ViewVersion currentVersion()` | Get the current version | @Public |
| versions | `Iterable<ViewVersion> versions()` | Get the versions of this view | @Public |
| version | `ViewVersion version(int versionId)` | Get a version by ID | @Public |
| history | `List<ViewHistoryEntry> history()` | Get the version history | @Public |
| properties | `Map<String, String> properties()` | Return properties map | @Public |
| location | `default String location()` | Return the view's base location | @Public |
| updateProperties | `UpdateViewProperties updateProperties()` | Create UpdateViewProperties | @Public |
| replaceVersion | `default ReplaceViewVersion replaceVersion()` | Create ReplaceViewVersion | @Public |
| updateLocation | `default UpdateLocation updateLocation()` | Create UpdateLocation | @Public |
| uuid | `default UUID uuid()` | Returns the view's UUID | @Public |
| sqlFor | `default SQLViewRepresentation sqlFor(String dialect)` | Get SQL for dialect | @Public |

**Method Count**: 14

---

### 5.2 ViewVersion Interface (@Public)

**Description**: A version of a view.

| Method | Signature | Description |
|--------|-----------|-------------|
| versionId | `int versionId()` | Returns version ID |
| timestampMillis | `long timestampMillis()` | Returns timestamp |
| summary | `Map<String, String> summary()` | Returns summary |
| representations | `List<ViewRepresentation> representations()` | Returns representations |
| schemaId | `int schemaId()` | Returns schema ID |
| defaultCatalog | `String defaultCatalog()` | Returns default catalog |
| defaultNamespace | `Namespace defaultNamespace()` | Returns default namespace |

---

### 5.3 SQLViewRepresentation Interface (@Public)

**Description**: SQL representation of a view.

| Method | Signature | Description |
|--------|-----------|-------------|
| sql | `String sql()` | Returns the SQL query |
| dialect | `String dialect()` | Returns the SQL dialect |

---

## 6. New Features

### 6.1 VariantType (@Public, New in 1.9+)

**Description**: Semi-structured data type supporting flexible schema evolution.

| Method | Signature | Description |
|--------|-----------|-------------|
| typeId | `Type.TypeID typeId()` | Returns VARIANT |

**Usage**: Supports storing JSON-like semi-structured data with flexible schema. Can store any Iceberg type within a single column.

---

### 6.2 PartitionStatisticsFile (@Public)

**Description**: Partition-level statistics for efficient query planning.

| Method | Signature | Description |
|--------|-----------|-------------|
| snapshotId | `long snapshotId()` | Associated snapshot ID |
| path | `String path()` | File path |
| fileSizeInBytes | `long fileSizeInBytes()` | File size |

---

### 6.3 GeographyType and GeometryType (@Public, New)

**Description**: Geographic data types for location-aware applications.

| Method | Signature | Description |
|--------|-----------|-------------|
| typeId | `Type.TypeID typeId()` | Returns GEOGRAPHY or GEOMETRY |

---

### 6.4 TimestampNanoType (@Public, New)

**Description**: Nanosecond precision timestamp type.

| Method | Signature | Description |
|--------|-----------|-------------|
| typeId | `Type.TypeID typeId()` | Returns TIMESTAMP_NANO |

---

## 7. Other Important Interfaces

### 7.1 PendingUpdate Interface (@Public)

**Description**: Base interface for pending updates.

| Method | Signature | Description |
|--------|-----------|-------------|
| apply | `T apply()` | Apply and return the update |
| commit | `void commit()` | Commit the update |
| updateEvent | `default UpdateEvent updateEvent()` | Returns update event |

---

### 7.2 SnapshotUpdate Interface (@Public)

**Description**: Base interface for updates that produce snapshots.

| Method | Signature | Description |
|--------|-----------|-------------|
| deleteWith | `T deleteWith(Consumer<String> deleteFunc)` | Set delete callback |
| scanManifestsWith | `T scanManifestsWith(ExecutorService executor)` | Use executor |
| set | `T set(String key, String value)` | Set snapshot property |
| stageOnly | `T stageOnly()` | Stage without commit |
| toBranch | `T toBranch(String branch)` | Commit to branch |

---

### 7.3 Scan Interface (@Public)

**Description**: Base interface for scan operations.

| Method | Signature | Description |
|--------|-----------|-------------|
| filter | `Expression filter()` | Returns filter expression |
| caseSensitive | `T caseSensitive(boolean caseSensitive)` | Set case sensitivity |
| project | `T project(Schema schema)` | Project schema |
| select | `T select(String... columns)` | Select columns |
| planFiles | `CloseableIterable<F> planFiles()` | Plan files |
| planTasks | `CloseableIterable<T> planTasks()` | Plan tasks |

---

### 7.4 ContentFile Interface (@Public)

**Description**: Base interface for content files (data and delete).

| Method | Signature | Description |
|--------|-----------|-------------|
| content | `FileContent content()` | Returns file content type |
| path | `String path()` | Returns file path |
| format | `FileFormat format()` | Returns file format |
| partition | `StructLike partition()` | Returns partition |
| recordCount | `long recordCount()` | Returns record count |
| fileSizeInBytes | `long fileSizeInBytes()` | Returns file size |

---

### 7.5 UpdatePartitionSpec Interface (@Public)

**Description**: API for updating partition spec.

| Method | Signature | Description |
|--------|-----------|-------------|
| addField | `UpdatePartitionSpec addField(String name)` | Add partition field |
| addField | `UpdatePartitionSpec addField(String name, Transform transform)` | Add with transform |
| removeField | `UpdatePartitionSpec removeField(String name)` | Remove partition field |
| renameField | `UpdatePartitionSpec renameField(String name, String newName)` | Rename field |

---

### 7.6 UpdateProperties Interface (@Public)

**Description**: API for updating table properties.

| Method | Signature | Description |
|--------|-----------|-------------|
| set | `UpdateProperties set(String key, String value)` | Set property |
| remove | `UpdateProperties remove(String key)` | Remove property |

---

## 8. Summary Statistics

| Module | Interface/Class | Method Count | Stability |
|--------|----------------|--------------|-----------|
| **Table API** | Table | 38 | @Public |
| | TableScan | 26 | @Public |
| | Transaction | 19 | @Public |
| | UpdateSchema | 33 | @Public |
| | AppendFiles | 9 | @Public |
| | DeleteFiles | 12 | @Public |
| | OverwriteFiles | 12 | @Public |
| | RewriteFiles | 12 | @Public |
| | RowDelta | 12 | @Public |
| | ExpireSnapshots | 12 | @Public |
| | ManageSnapshots | 17 | @Public |
| | DataFile | 28 | @Public |
| | DeleteFile | 28 | @Public |
| | FileScanTask | 14 | @Public |
| | Snapshot | 18 | @Public |
| | PartitionStatisticsFile | 3 | @Public |
| **Catalog API** | Catalog | 23 | @Public |
| | Catalog.TableBuilder | 8 | @Public |
| | TableIdentifier | 10 | @Public |
| | Namespace | 9 | @Public |
| **Types API** | Type | 13 | @Public |
| | Types (including nested) | ~50 | @Public |
| **Expressions API** | Expressions | 62 | @Public |
| | Expression | - | @Public |
| | UnboundPredicate | 3 | @Public |
| | Literal | 2 | @Public |
| **View API** | View | 14 | @Public |
| | ViewVersion | 7 | @Public |
| | SQLViewRepresentation | 2 | @Public |
| **New Features** | VariantType | 1 | @Public |
| | GeographyType | 1 | @Public |
| | GeometryType | 1 | @Public |
| | TimestampNanoType | 1 | @Public |
| **Other** | PendingUpdate | 3 | @Public |
| | SnapshotUpdate | 5 | @Public |
| | Scan | 7 | @Public |
| | ContentFile | 6 | @Public |
| | UpdatePartitionSpec | 4 | @Public |
| | UpdateProperties | 2 | @Public |

---

## Total Method Count Summary

| Category | Total Methods |
|----------|---------------|
| Table API | 262 |
| Catalog API | 50 |
| Types API | ~63 |
| Expressions API | 67 |
| View API | 23 |
| New Features | 4 |
| Other Important | 27 |
| **Grand Total** | **~433** |

---

## API Stability Annotations

- **@Public**: Stable API, safe for external use
- **@Private**: Internal API, subject to change
- **@Experimental**: New API, may evolve
- **@Deprecated**: Scheduled for removal

---

## References

- Official Documentation: https://iceberg.apache.org/docs/latest/api/
- Javadoc: https://iceberg.apache.org/javadoc/latest/
- GitHub: https://github.com/apache/iceberg

---

**Document End**