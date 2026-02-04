# Tower Switch Ratio Query - Technical Logic & Workflow

## Core Query Logic

**Formula**: `switch_ratio = total_switches / num_calls`  
**Calculation**: For each call, `switches = tower_count - 1` (minimum 0)

---

## Key Technical Terms by Engine

### PostgreSQL
- **Single-pass aggregation** with nested GROUP BY
- **LEFT JOIN** for call-to-tower correlation
- **CTE (Common Table Expression)** for bi-signal event pairing
- **DECIMAL casting** for precise ratio calculation
- **No state management** - processes entire dataset in single query

### Spark Structured Streaming
- **Micro-batch processing** (50-200ms intervals)
- **Watermark**: 1-minute late data tolerance
- **Tumbling windows**: 10-second non-overlapping windows
- **Output mode**: `append` (emit only when watermark passes)
- **Two-stage aggregation**: partial (per-call) → total (per-user)
- **Stream-stream join** with watermarks on both sides
- **State backend**: In-memory HashMaps with checkpoint to HDFS/local disk
- **Checkpointing**: Periodic snapshots for fault tolerance

### Flink DataStream API
- **Event-at-a-time processing** (per-record)
- **Watermark**: 5-second bounded out-of-orderness
- **Tumbling event-time windows**: 10-second windows
- **Keyed state**: RocksDB for stateful aggregations
- **AggregateFunction**: incremental per-call tower counting
- **reduceGroup**: final per-user aggregation
- **State backend**: RocksDB (embedded key-value store) - persists to local disk
- **Checkpointing**: Asynchronous snapshots with exactly-once semantics

---

## Windowing & Watermark Visualization

### Tumbling Windows (10-second windows)

```
Time ────────────────────────────────────────────────────────────────►
     0s      10s     20s     30s     40s     50s     60s

     ├───────┤───────┤───────┤───────┤───────┤───────┤
     │ Win 1 │ Win 2 │ Win 3 │ Win 4 │ Win 5 │ Win 6 │
     └───────┘───────┘───────┘───────┘───────┘───────┘
     
     Non-overlapping windows - each event belongs to EXACTLY ONE window
     Window assigned based on event_time (not processing time)
```

### Watermark Progression Example

```
Event Time vs Processing Time with Watermark
─────────────────────────────────────────────

Events Arrive:
    Event_Time  Processing_Time  Watermark  Status
    ──────────  ───────────────  ─────────  ──────
    10:00:05    10:00:10         10:00:05   ✅ On-time
    10:00:08    10:00:11         10:00:08   ✅ On-time
    10:00:12    10:00:13         10:00:12   ✅ On-time
    10:00:15    10:00:16         10:00:15   ✅ On-time
    
    ⏱️  Watermark at 10:00:15
    
    10:00:09    10:00:17         10:00:15   ✅ Late (within tolerance)
    10:00:04    10:00:18         10:00:15   ❌ Too late (dropped)
    
    Spark: 1-minute tolerance  → accept events up to 10:00:15 - 1:00 = 9:59:15
    Flink: 5-second tolerance  → accept events up to 10:00:15 - 0:05 = 10:00:10
```

### Watermark Mechanics - Spark vs Flink

```
┌─────────────────────────────────────────────────────────────┐
│                    SPARK WATERMARK (1 minute)               │
└─────────────────────────────────────────────────────────────┘

Time:     10:00:00    10:00:10    10:00:20    10:00:30
          ├──────────┼──────────┼──────────┼──────────►
Windows:  │  Win 1   │  Win 2   │  Win 3   │  Win 4   │
          [00-10s]   [10-20s]   [20-30s]   [30-40s]   
          
Watermark progression (current_max_event_time - 1 minute):
          
t=10:00:10  →  Watermark: 9:59:10  (nothing emitted yet)
t=10:00:20  →  Watermark: 9:59:20  (nothing emitted yet)
t=10:01:15  →  Watermark: 10:00:15 (Win 1 [00-10s] complete! Emit results)
t=10:01:25  →  Watermark: 10:00:25 (Win 2 [10-20s] complete! Emit results)

Late event at 10:00:05 arriving at t=10:01:20:
  ✅ Accepted (within 1-minute tolerance)
  → Added to Win 1 state before emission

Late event at 10:00:05 arriving at t=10:02:30:
  ❌ Dropped (watermark already passed 10:01:30)


┌─────────────────────────────────────────────────────────────┐
│                   FLINK WATERMARK (5 seconds)               │
└─────────────────────────────────────────────────────────────┘

Time:     10:00:00    10:00:10    10:00:20    10:00:30
          ├──────────┼──────────┼──────────┼──────────►
Windows:  │  Win 1   │  Win 2   │  Win 3   │  Win 4   │
          [00-10s]   [10-20s]   [20-30s]   [30-40s]   
          
Watermark progression (current_max_event_time - 5 seconds):
          
t=10:00:10  →  Watermark: 10:00:05  (nothing emitted)
t=10:00:15  →  Watermark: 10:00:10  (Win 1 [00-10s] complete! Trigger window)
t=10:00:25  →  Watermark: 10:00:20  (Win 2 [10-20s] complete! Trigger window)

Late event at 10:00:08 arriving at t=10:00:14:
  ✅ Accepted (watermark at 10:00:10, event at 10:00:08 → within 5s)
  → Processed by Win 1

Late event at 10:00:04 arriving at t=10:00:16:
  ❌ Dropped (watermark at 10:00:10, event at 10:00:04 → outside 5s window)
```

### Window State Management

```
┌──────────────────────────────────────────────────────────────────┐
│                  SPARK STATE MANAGEMENT                          │
└──────────────────────────────────────────────────────────────────┘

In-Memory State Store (per micro-batch):
┌─────────────────────────────────────────┐
│  State Store (HashMap)                  │
│  ┌────────────────────────────────────┐ │
│  │ Key: (window, call_id)             │ │
│  │ Value: tower_count                 │ │
│  ├────────────────────────────────────┤ │
│  │ ([10:00:00-10:00:10], 12345) → 3  │ │
│  │ ([10:00:00-10:00:10], 67890) → 2  │ │
│  │ ([10:00:10-10:00:20], 11111) → 1  │ │
│  └────────────────────────────────────┘ │
└─────────────────────────────────────────┘
         │
         │ Checkpoint every 10 seconds
         ▼
┌─────────────────────────────────────────┐
│  HDFS / Local Disk Checkpoint           │
│  (/tmp/checkpoints/...)                 │
│  - State snapshot                       │
│  - Offset metadata                      │
│  - Schema information                   │
└─────────────────────────────────────────┘

When window completes (watermark passes):
  → Final aggregation
  → Emit results to output sink
  → Clear window state from memory


┌──────────────────────────────────────────────────────────────────┐
│                  FLINK STATE MANAGEMENT                          │
└──────────────────────────────────────────────────────────────────┘

RocksDB State Backend (embedded key-value store):
┌─────────────────────────────────────────┐
│  RocksDB (on-disk, LSM-tree)            │
│  ┌────────────────────────────────────┐ │
│  │ Keyed State                        │ │
│  │ Key: (key, namespace, state_name)  │ │
│  │ Value: serialized state            │ │
│  ├────────────────────────────────────┤ │
│  │ (caller="90001", win=[00-10], cnt) │ │
│  │   → AggregatorState(numCalls=2,    │ │
│  │                     totalSw=5)     │ │
│  ├────────────────────────────────────┤ │
│  │ (caller="90002", win=[00-10], cnt) │ │
│  │   → AggregatorState(numCalls=1,    │ │
│  │                     totalSw=2)     │ │
│  └────────────────────────────────────┘ │
│                                         │
│  Filesystem: /tmp/flink-state/          │
│  Format: SST files (sorted string table)│
└─────────────────────────────────────────┘
         │
         │ Asynchronous checkpoint (every N seconds)
         ▼
┌─────────────────────────────────────────┐
│  Checkpoint Storage (HDFS/S3/Local)     │
│  - Full RocksDB snapshot                │
│  - Operator state                       │
│  - Exactly-once semantics               │
└─────────────────────────────────────────┘

When window fires (watermark triggers):
  → WindowFunction/ProcessWindowFunction invoked
  → Aggregate state read from RocksDB
  → Emit results
  → Clear window state (garbage collection)
```

### Stream-Stream Join with Watermarks (Spark)

```
┌──────────────────────────────────────────────────────────────────┐
│              STREAM-STREAM JOIN VISUALIZATION                    │
└──────────────────────────────────────────────────────────────────┘

mono_signal stream:        tower_signal stream:
┌──────────────┐           ┌──────────────┐
│ event_time   │           │ event_time   │
│ 10:00:05     │           │ 10:00:06     │
│ call_id=1001 │           │ call_id=1001 │
│ caller=90001 │           │ tower=T1     │
└──────┬───────┘           └──────┬───────┘
       │                          │
       │ withWatermark("1 min")   │ withWatermark("1 min")
       ▼                          ▼
 ┌──────────────┐           ┌──────────────┐
 │ Buffered in  │           │ Buffered in  │
 │ state store  │           │ state store  │
 └──────┬───────┘           └──────┬───────┘
        │                          │
        │                          │
        └────────────┬─────────────┘
                     │ JOIN on (call_id, window)
                     ▼
            ┌──────────────────┐
            │ Joined Stream    │
            │ call_id=1001     │
            │ caller=90001     │
            │ tower=T1         │
            │ window=[00-10s]  │
            └──────────────────┘

State Retention:
- Spark keeps buffered events for 1 minute after watermark
- If tower event arrives late (within 1 min), join still succeeds
- After 1 minute, left side events are flushed (LEFT JOIN preserves them)
```

### Incremental Aggregation (Flink AggregateFunction)

```
┌──────────────────────────────────────────────────────────────────┐
│         FLINK AGGREGATEFUNCTION (Incremental Updates)            │
└──────────────────────────────────────────────────────────────────┘

Window: [10:00:00 - 10:00:10]  Key: call_id=1001

Event Arrival Sequence:
────────────────────────

1️⃣  Event: (call_id=1001, tower=T1, timestamp=10:00:02)
    
    createAccumulator() → (callId=0, count=0)
    add(event, acc)     → (callId=1001, count=1)
    
    State: [callId=1001, towerCount=1]  ✅ Stored in RocksDB

2️⃣  Event: (call_id=1001, tower=T2, timestamp=10:00:05)
    
    (read existing accumulator from state)
    add(event, acc)     → (callId=1001, count=2)
    
    State: [callId=1001, towerCount=2]  ✅ Updated in RocksDB

3️⃣  Event: (call_id=1001, tower=T1, timestamp=10:00:08)
    
    (read existing accumulator from state)
    add(event, acc)     → (callId=1001, count=3)
    
    State: [callId=1001, towerCount=3]  ✅ Updated in RocksDB

4️⃣  Watermark arrives at 10:00:15 → Window fires!
    
    getResult(acc)      → (callId=1001, towerCount=3, switches=2)
    
    Output: (call_id=1001, tower_count=3, switches=2)  📤 Emitted

Advantages:
✅ Memory efficient - only stores accumulator (not all events)
✅ Low latency - incrementally updates on each event
✅ Scales well - constant memory per key regardless of event count
```

---

## State Backend Comparison

| Feature | Spark (HashMapStateStore) | Flink (RocksDB) |
|---------|---------------------------|-----------------|
| **Storage** | In-memory HashMap | On-disk LSM-tree |
| **Persistence** | Checkpoint to HDFS/disk | Embedded in RocksDB + checkpoint |
| **Memory Usage** | High (all state in heap) | Low (only hot data in memory) |
| **Scalability** | Limited by JVM heap | Limited by disk space |
| **Access Speed** | Fast (in-memory) | Medium (disk I/O, cached reads) |
| **State Size** | ~10GB per executor | ~TB scale per task manager |
| **Use Case** | Small-medium state | Large stateful applications |

---

## Workflow Diagrams

### 1. PostgreSQL CSV (Mono-Signal)
```
┌─────────────────┐
│ mono_signal.csv │ (call_id, caller, callee...)
└────────┬────────┘
         │ COPY FROM
         ▼
┌─────────────────┐         ┌──────────────────┐
│  CallRecords    │         │ tower_signal.csv │
│   (loaded)      │◄────┬───┤  (call_id, tower)│
└────────┬────────┘     │   └──────────────────┘
         │              │ COPY FROM
         │              ▼
         │   ┌──────────────────┐
         │   │  TowerSignals    │
         │   │    (loaded)      │
         │   └────────┬─────────┘
         │            │
         │            │ LEFT JOIN (on call_id)
         └────────────┤
                      ▼
            ┌──────────────────────┐
            │  GROUP BY caller     │
            │  COUNT(call_id)      │
            │  COUNT(tower)        │
            │  ratio = COUNT/COUNT │
            └────────┬─────────────┘
                     ▼
              ┌──────────────┐
              │ Result Table │
              └──────────────┘
```

**Key Steps**:
1. Load CSVs into PostgreSQL tables
2. Execute single SQL query with LEFT JOIN + GROUP BY
3. Return aggregated results

---

### 2. PostgreSQL CSV (Bi-Signal)
```
┌─────────────────┐
│  bi_signal.csv  │ (call_id, event_type=0/1, caller...)
└────────┬────────┘
         │ COPY FROM
         ▼
┌──────────────────┐      ┌────────────────────┐
│ BiCallRecords    │      │tower_signal_bi.csv │
│  (START/END)     │      │ (event_type=0/1)   │
└────────┬─────────┘      └─────────┬──────────┘
         │                          │ COPY FROM
         │                          ▼
         │                ┌──────────────────┐
         │                │ BiTowerSignals   │
         │                └────────┬─────────┘
         │                         │
         ▼                         │
┌──────────────────────┐           │
│ WITH call_pairs AS ( │           │
│   SELF-JOIN          │           │
│   START ⟷ END        │           │
│ )                    │           │
└────────┬─────────────┘           │
         │                         │
         │  LEFT JOIN (on call_id) │
         └─────────────┬───────────┘
                       ▼
             ┌──────────────────────┐
             │  GROUP BY caller     │
             │  COUNT(call_id)      │
             │  COUNT(tower)        │
             │  ratio = COUNT/COUNT │
             └────────┬─────────────┘
                      ▼
               ┌──────────────┐
               │ Result Table │
               └──────────────┘
```

**Key Steps**:
1. Load bi-signal CSVs (START/END events)
2. CTE: Self-join BiCallRecords (START ⟷ END) to reconstruct calls
3. LEFT JOIN with BiTowerSignals
4. GROUP BY + aggregate

---

### 3. Spark CSV (Mono-Signal)
```
┌─────────────────┐         ┌──────────────────┐
│ mono_signal.csv │         │ tower_signal.csv │
└────────┬────────┘         └────────┬─────────┘
         │ read.csv()              │ read.csv()
         ▼                         ▼
  ┌────────────┐           ┌─────────────────┐
  │  mono_df   │           │   tower_df      │
  │ (call_id,  │           │ (call_id, tower)│
  │  caller)   │           └────────┬────────┘
  └─────┬──────┘                    │
        │                           │ groupBy(call_id)
        │                           │ count(tower)
        │                           ▼
        │                  ┌──────────────────────┐
        │                  │ tower_counts         │
        │                  │ (call_id, tower_cnt) │
        │                  └────────┬─────────────┘
        │                           │ switches = max(cnt-1, 0)
        │                           ▼
        │                  ┌──────────────────────┐
        │                  │ call_switches        │
        │                  │ (call_id, switches)  │
        │                  └────────┬─────────────┘
        │                           │
        │  LEFT JOIN (on call_id)   │
        └───────────────┬───────────┘
                        ▼
              ┌──────────────────────┐
              │  caller_switches     │
              │ (caller, switches)   │
              └────────┬─────────────┘
                       │ groupBy(caller)
                       │ sum(switches)
                       ▼
            ┌────────────────────────┐
            │ Final Aggregation      │
            │ num_calls, tot_switches│
            │ ratio = total/num_calls│
            └────────┬───────────────┘
                     ▼
              ┌──────────────┐
              │ CSV Output   │
              └──────────────┘
```

**Key Steps**:
1. **Read**: Load CSVs into DataFrames
2. **Partial Aggregate**: Group towers by call_id → count → calculate switches
3. **Join**: LEFT JOIN mono_df ⟷ call_switches
4. **Total Aggregate**: Group by caller → sum switches, count calls
5. **Write**: Output to CSV with header

---

### 4. Spark Kafka (Mono-Signal)
```
┌──────────────────┐       ┌──────────────────┐
│ Kafka Topic:     │       │ Kafka Topic:     │
│ mono_signal      │       │ tower_signal     │
└────────┬─────────┘       └────────┬─────────┘
         │ readStream()            │ readStream()
         ▼                         ▼
  ┌────────────┐           ┌─────────────────┐
  │ mono_df    │           │   tower_df      │
  │ JSON parse │           │   JSON parse    │
  └─────┬──────┘           └────────┬────────┘
        │                           │
        │ withWatermark("1 min")    │ withWatermark("1 min")
        ▼                           ▼
  ┌────────────┐           ┌─────────────────┐
  │ mono_water-│           │ tower_watermarked│
  │  marked    │           └────────┬────────┘
  └─────┬──────┘                    │
        │                           │ groupBy(window, call_id)
        │                           │ count(tower)
        │                           ▼
        │                  ┌──────────────────────┐
        │                  │ tower_counts (windowed)│
        │                  │ 10-sec tumbling window│
        │                  └────────┬─────────────┘
        │                           │ switches = max(cnt-1, 0)
        │                           ▼
        │                  ┌──────────────────────┐
        │                  │ call_switches        │
        │                  └────────┬─────────────┘
        │                           │
        │  JOIN (call_id + window)  │
        └───────────────┬───────────┘
                        ▼
              ┌──────────────────────┐
              │  joined (windowed)   │
              │ (window, caller,     │
              │  call_id, switches)  │
              └────────┬─────────────┘
                       │ groupBy(window, caller)
                       │ sum(switches), count(calls)
                       ▼
            ┌────────────────────────┐
            │ Final Aggregation      │
            │ per 10-sec window      │
            │ ratio = total/num_calls│
            └────────┬───────────────┘
                     │
                     │ writeStream.outputMode("append")
                     ▼
              ┌──────────────┐
              │ CSV Output   │
              │ (micro-batch)│
              └──────────────┘
```

**Key Steps**:
1. **Read**: `readStream` from Kafka topics (JSON format)
2. **Watermark**: Apply 1-minute watermark on both streams
3. **Window**: 10-second tumbling windows on event_time
4. **Partial Aggregate**: Group towers by (window, call_id) → count
5. **Join**: Stream-stream join on (call_id, window)
6. **Total Aggregate**: Group by (window, caller) → sum, count
7. **Write**: `append` mode (emit when watermark passes)

**Key Terms**: 
- `outputMode("append")` - only complete windows
- `withWatermark()` - handles late data
- Stream-stream join requires watermarks on BOTH sides

---

### 5. Flink CSV (Mono-Signal)
```
┌─────────────────┐         ┌──────────────────┐
│ mono_signal.csv │         │ tower_signal.csv │
└────────┬────────┘         └────────┬─────────┘
         │ readTextFile()          │ readTextFile()
         ▼                         ▼
  ┌────────────┐           ┌─────────────────┐
  │  DataSet   │           │   DataSet       │
  │ (lines)    │           │   (lines)       │
  └─────┬──────┘           └────────┬────────┘
        │ map(parse CSV)            │ map(parse CSV)
        ▼                           ▼
  ┌────────────┐           ┌─────────────────┐
  │ CallRecord │           │ TowerSignal     │
  │  POJO      │           │  POJO           │
  └─────┬──────┘           └────────┬────────┘
        │                           │
        │                           │ groupBy(call_id).sum()
        │                           │ count towers
        │                           ▼
        │                  ┌──────────────────────┐
        │                  │ tower_counts         │
        │                  │ (call_id, count)     │
        │                  └────────┬─────────────┘
        │                           │ map: switches = max(cnt-1,0)
        │                           ▼
        │                  ┌──────────────────────┐
        │                  │ call_switches        │
        │                  └────────┬─────────────┘
        │                           │
        │  leftOuterJoin(call_id)   │
        └───────────────┬───────────┘
                        ▼
              ┌──────────────────────┐
              │ caller_switches      │
              │ (caller, call_id,    │
              │  switches)           │
              └────────┬─────────────┘
                       │ groupBy(caller)
                       │ reduceGroup (iterate)
                       ▼
            ┌────────────────────────┐
            │ Stateful Aggregation   │
            │ accumulate: numCalls++ │
            │             totalSwitch│
            │ ratio = total/num_calls│
            └────────┬───────────────┘
                     ▼
              ┌──────────────┐
              │ CSV Output   │
              │ with header  │
              └──────────────┘
```

**Key Steps**:
1. **Read**: `readTextFile()` → batch DataSet
2. **Parse**: Map CSV lines to POJOs
3. **Partial Aggregate**: `groupBy(call_id).sum(1)` → tower counts
4. **Transform**: Map to switches (tower_count - 1)
5. **Join**: `leftOuterJoin` on call_id
6. **Total Aggregate**: `reduceGroup` with manual iteration
7. **Write**: `writeAsText()` with header

**Key Terms**:
- **DataSet API** (batch mode for CSV)
- **reduceGroup** (iterate all records per key)
- **leftOuterJoin** (preserve all calls even without towers)

---

### 6. Flink Kafka (Mono-Signal)
```
┌──────────────────┐       ┌──────────────────┐
│ Kafka Topic:     │       │ Kafka Topic:     │
│ mono_signal      │       │ tower_signal     │
└────────┬─────────┘       └────────┬─────────┘
         │ fromSource()            │ fromSource()
         │ KafkaSource             │ KafkaSource
         ▼                         ▼
  ┌────────────┐           ┌─────────────────┐
  │ DataStream │           │   DataStream    │
  │ (JSON)     │           │   (JSON)        │
  └─────┬──────┘           └────────┬────────┘
        │ WatermarkStrategy          │ WatermarkStrategy
        │ (5-sec bounded)            │ (5-sec bounded)
        ▼                           ▼
  ┌────────────┐           ┌─────────────────┐
  │ mono_stream│           │ tower_stream    │
  │ with       │           │ with watermarks │
  │ watermarks │           └────────┬────────┘
  └─────┬──────┘                    │
        │                           │ keyBy(call_id)
        │                           │ window(10-sec tumbling)
        │                           │ aggregate (incremental)
        │                           ▼
        │                  ┌──────────────────────┐
        │                  │ tower_counts         │
        │                  │ AggregateFunction    │
        │                  │ (call_id, count,     │
        │                  │  switches)           │
        │                  └────────┬─────────────┘
        │                           │
        │  keyBy(call_id)           │
        │  window(10-sec tumbling)  │
        └───────────────┬───────────┘
                        │ intervalJoin (event-time)
                        │ or CoProcessFunction
                        ▼
              ┌──────────────────────┐
              │ Joined Stream        │
              │ (caller, call_id,    │
              │  switches, window)   │
              └────────┬─────────────┘
                       │ keyBy(caller)
                       │ window(10-sec tumbling)
                       │ aggregate (AggregateFunction)
                       ▼
            ┌────────────────────────┐
            │ Final Aggregation      │
            │ Keyed State (RocksDB)  │
            │ accumulate per window  │
            │ ratio = total/num_calls│
            └────────┬───────────────┘
                     │
                     │ print() or sink
                     ▼
              ┌──────────────┐
              │ Output File  │
              │ (per window) │
              └──────────────┘
```

**Key Steps**:
1. **Read**: `fromSource()` with KafkaSource
2. **Watermark**: `WatermarkStrategy.forBoundedOutOfOrderness(5s)`
3. **Window**: `TumblingEventTimeWindows.of(10s)` on event_time
4. **Partial Aggregate**: `keyBy(call_id).window().aggregate()` → tower counts
5. **Join**: Event-time interval join or CoProcessFunction
6. **Total Aggregate**: `keyBy(caller).window().aggregate()` → final stats
7. **Write**: Sink to file or stdout

**Key Terms**:
- **DataStream API** (streaming mode)
- **AggregateFunction** (incremental pre-aggregation)
- **Keyed state** (RocksDB backend)
- **Event-time windows** with watermarks
- **Bounded out-of-orderness** (5-second tolerance)

---

## Critical Differences: CSV vs Kafka

| Aspect | CSV Mode | Kafka Mode |
|--------|----------|------------|
| **Spark API** | DataFrame (batch) | Structured Streaming |
| **Spark Output** | `.write.csv()` | `.writeStream.outputMode("append")` |
| **Spark State** | No state management | In-memory StateStore + checkpointing |
| **Flink API** | DataSet (batch) | DataStream (streaming) |
| **Flink Aggregation** | `reduceGroup` (iterate) | `AggregateFunction` (incremental) |
| **Flink State** | No state backend | RocksDB (embedded LSM-tree) |
| **Windowing** | N/A (entire dataset) | 10-second tumbling windows |
| **Watermarks** | N/A | Spark: 1 min, Flink: 5 sec |
| **Join Strategy** | Static join | Stream-stream join (time-bounded) |
| **Fault Tolerance** | Task retry | Checkpointing + exactly-once semantics |

---

## Output Modes Explained
 (50-200ms batch intervals)
- **Structured Streaming API** (high-level DataFrame/SQL)
- **Watermark** (1-minute late data tolerance)
- **Tumbling windows** (10-second non-overlapping)
- **Output mode: append** (emit only complete windows)
- **Stream-stream join** (requires watermarks on both sides)
- **StateStore** (in-memory HashMap with HDFS checkpointing)
- **Checkpointing** (fault tolerance via periodic snapshots)

### Flink Terms:
- **Event-at-a-time processing** (per-record latency)
- **DataStream API** (low-level streaming primitives)
- **Keyed state** (partitioned by key, stored in RocksDB)
- **RocksDB state backend** (embedded LSM-tree, disk-backed)
- **AggregateFunction** (incremental pre-aggregation)
- **TumblingEventTimeWindows** (10-second windows)
- **WatermarkStrategy** (5-second bounded out-of-orderness)
- **Event-time triggers** (fire when watermark passes window end)
- **Exactly-once semantics** (via checkpointing + two-phase commit)

### PostgreSQL Terms:
- **LEFT JOIN** (preserve all calls, even without tower data)
- **GROUP BY** (aggregate by caller)
- **CTE (WITH clause)** for bi-signal START/END pairing
- **SELF-JOIN** (BiCallRecords event_type=0 ⟷ event_type=1)
- **Single-pass aggregation** (no intermediate state)
- **No windowing** (processes entire dataset at once) update after closure
    ✅ Our case: Once a 10-sec window closes, results are final

2️⃣  UPDATE MODE (Not used, but for comparison)
    ───────────────────────────────────────────
    Emit updated rows as they change
    
    t=10:00:05  →  📤 Emit partial Win 1 result (user1: 1 call)
    t=10:00:08  →  📤 Update Win 1 result (user1: 2 calls)
    t=10:00:10  →  📤 Final Win 1 result (user1: 3 calls)
    
    ❌ Not suitable: Too many intermediate updates for windowed aggregation

3️⃣  COMPLETE MODE (Not used)
    ──────────────────────────
    Emit entire result table every trigger
    
    Every micro-batch: 📤 Emit ALL results (all windows, all users)
    
    ❌ Not suitable: Unbounded result growth, memory issues
```

### Flink Window Triggers

```
┌──────────────────────────────────────────────────────────────────┐
│                   FLINK WINDOW TRIGGERS                          │
└──────────────────────────────────────────────────────────────────┘

Event-Time Trigger (Used in our query):
──────────────────────────────────────

Window [10:00:00 - 10:00:10]:

Events arrive: 10:00:02, 10:00:05, 10:00:08
Watermark progression: 10:00:02 → 10:00:05 → 10:00:08 → 10:00:15

When watermark reaches 10:00:10 (window end time):
  ✅ Window fires!
  ✅ WindowFunction/ProcessWindowFunction invoked
  ✅ Aggregate result emitted
  ✅ State cleared for this window

Late event at 10:00:07 arrives after watermark at 10:00:15:
  ✅ If within allowed lateness (5 sec): Accepted, window re-fires
  ❌ If outside allowed lateness: Dropped

Processing-Time Trigger (Not used):
─────────────────────────────────
Fires based on wall-clock time, not event timestamps
❌ Not suitable for our use case (need event-time correctness)
```

---

## Key Terms Summary (For Explaining)

### Spark Terms:
- **Micro-batch processing**, **Structured Streaming**
- **Watermark** (late data handling)
- **Tumbling windows** (non-overlapping)
- **Output mode: append** (complete windows only)
- **Stream-stream join** (requires watermarks on both)

### Flink Terms:
- **Event-at-a-time**, **DataStream API**
- **Keyed state** (RocksDB backend)
- **AggregateFunction** (incremental aggregation)
- **TumblingEventTimeWindows**
- **WatermarkStrategy** (bounded out-of-orderness)
- **reduceGroup** (batch), **process** (streaming)

### PostgreSQL Terms:
- **LEFT JOIN**, **GROUP BY**
- **CTE (WITH clause)** for bi-signal pairingState Management | Best For |
|--------|---------|------------|-------------|------------------|----------|
| PostgreSQL | **Lowest** (37ms) | **Highest** (26k rec/s) | Limited (O(n²) on bi-signal) | None (stateless) | Small datasets (<10k) |
| Flink | Medium (1.3s) | Medium (798 rec/s) | **Best** (sub-linear) | RocksDB (disk-backed) | Medium/large datasets |
| Spark | High (6.4s) | Low (157 rec/s) | Good (handles skew) | In-memory + checkpoint | Very large datasets (>100k) |

---

## Architecture Summary

```
┌────────────────────────────────────────────────────────────────┐
│                    SYSTEM ARCHITECTURE                         │
└────────────────────────────────────────────────────────────────┘

POSTGRESQL (Stateless Batch):
┌──────────────┐
│   CSV Files  │
└──────┬───────┘
       │ COPY
       ▼
┌──────────────┐     ┌──────────────┐
│  PostgreSQL  │────►│ Query Engine │
│   Tables     │     │ (no state)   │
└──────────────┘     └──────┬───────┘
                            │
                            ▼
                     ┌──────────────┐
                     │   Results    │
                     └──────────────┘

SPARK (Stateful Streaming):
┌──────────────┐
│ Kafka Topics │
└──────┬───────┘
       │ readStream
       ▼
┌──────────────────────┐     ┌─────────────────┐
│  Structured Stream   │────►│  StateStore     │
│  (DataFrames)        │     │  (in-memory)    │
└──────┬───────────────┘     └────────┬────────┘
       │                              │
       │ Window + Aggregate           │ Checkpoint
       ▼                              ▼
┌──────────────────────┐     ┌─────────────────┐
│  Micro-batch Engine  │────►│  HDFS/Disk      │
│  (every 50-200ms)    │     │  (recovery)     │
└──────┬───────────────┘     └─────────────────┘
       │
       │ writeStream (append mode)
       ▼
┌──────────────┐
│ CSV Output   │
└──────────────┘

FLINK (Stateful Streaming):
┌──────────────┐
│ Kafka Topics │
└──────┬───────┘
       │ fromSource (KafkaSource)
       ▼
┌──────────────────────┐     ┌─────────────────┐
│  DataStream          │────►│  RocksDB State  │
│  (event-at-a-time)   │     │  (on-disk)      │
└──────┬───────────────┘     └────────┬────────┘
       │                              │
       │ keyBy + window + aggregate   │ Async checkpoint
       ▼                              ▼
┌──────────────────────┐     ┌─────────────────┐
│  Window Operator     │────►│  HDFS/S3/Disk   │
│  (event-time trigger)│     │  (exactly-once) │
└──────┬───────────────┘     └─────────────────┘
       │
       │ sink (FileSink)
       ▼
┌──────────────┐
│ File Output  │
└──────────────┘
```

## Performance Characteristics

| Engine | Latency | Throughput | Scalability | Best For |
|--------|---------|------------|-------------|----------|
| PostgreSQL | **Lowest** (37ms) | **Highest** (26k rec/s) | Limited (O(n²) on bi-signal) | Small datasets (<10k) |
| Flink | Medium (1.3s) | Medium (798 rec/s) | **Best** (sub-linear) | Medium/large datasets |
| Spark | High (6.4s) | Low (157 rec/s) | Good (handles skew) | Very large datasets (>100k) |
