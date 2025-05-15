不知道大家在初次使用Flink的时候，是否对Flink中定义本地变量和状态比较好奇，这俩有啥区别？

而且在使用Window API时明明没有显式地创建状态，也没调用`getState()`，却依然把每个窗口里的所有元素都自动缓存到 StateBackend里，这到底是怎么做到的？它怎么可以自作主张呢。

### 本地变量 vs Managed State
我们先来看看第一个问题，怎么清楚的区分本地变量和状态的区别呢？我们看下表：

| 特性       | 本地变量（Local Variable / Field）                | Managed State (`ValueState`, `ListState`…)                |
|----------|----------------------------------------------|-----------------------------------------------------------|
| 生命周期    | 仅在当前 `processElement()` 或算子实例初始化时有效；Task 重启或 failover 后重置为初始值 | 跨事件、跨 checkpoint 持久化；重启后按最新 checkpoint 恢复 |
| 容错        | 不参与 Flink 容错；Task 重启或 Job 恢复后丢失            | 参与 checkpoint/savepoint；保证 Exactly-once 语义       |
| 序列化      | 不会被 Flink 自动序列化；只存在 JVM 堆栈或算子对象里         | 通过 `TypeSerializer` 序列化到 StateBackend（内存或 RocksDB） |
| 使用场景    | 临时计数、方法内临时缓存，无需跨事件保留        | 需要累积、聚合、窗口缓存、跨事件关联时使用                |

简单写个代码看看

```java
//本地字段无法容错 
public class MyMapFunction extends RichMapFunction<Event, Integer> {
    private int counter = 0;  // 普通字段

    @Override
    public Integer map(Event value) {
        counter += 1;
        return counter;       // 失败重启后，counter 会被重置为 0
    }
}

//使用 Managed State
public class MyStatefulMap extends RichMapFunction<Event, Integer> {
    private transient ValueState<Integer> counterState;

    @Override
    public void open(Configuration cfg) {
        counterState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("counter", Integer.class));
    }

    @Override
    public Integer map(Event value) throws Exception {
        Integer cnt = Optional.ofNullable(counterState.value()).orElse(0);
        cnt += 1;
        counterState.update(cnt);  // 这个值会被 checkpoint 序列化并恢复
        return cnt;
    }
}
```

### Window怎么存元素到state的？

Flink 的 Window 算子并没有让我们在代码里手动 `getState()`，我们一般都只是这样写：

```java
dataStream
  .keyBy(Event::getUserId)
  .window(TumblingEventTimeWindows.of(Time.minutes(5)))
  .apply(new ProcessWindowFunction<Event, Result, String, TimeWindow>() {
      @Override
      public void process(String key,
                          Context ctx,
                          Iterable<Event> elems,
                          Collector<Result> out) { … }
  });
```

却能自动把 5 分钟窗口里的所有 `Event` 缓存起来。而且在强哥之前的文章中也提到，如果Window定义的时间跨度太长，缓存在state里面的数据过多，可能会对服务性能造成影响，官网也是提到了的：
> Flink creates one copy of each element per window to which it belongs. Given this, tumbling windows keep one copy of each element (an element belongs to exactly one window unless it is dropped late). In contrast, sliding windows create several of each element, as explained in the Window Assigners section. Hence, a sliding window of size 1 day and slide 1 second might not be a good idea.

那么，Window背后究竟发生了什么？我们来看关键源码位置（注：以下源码基于flink-streaming-java:1.18.1）。

#### 1. 隐式注册StateDescriptor
我们先看示例代码
```
source
.map(new MapFunction<String, Tuple2<String, Integer>>() {
    @Override
    public Tuple2<String, Integer> map(String value) {
        return Tuple2.of(value, 1);
    }
})
.keyBy(value -> value.f0)
.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
.process(new WindowFunctionExample())
;

```
在执行`process`方法的时候，Flink会调用`org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorBuilder#apply(org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction<java.lang.Iterable<T>,R,K,W>)`，创建`WindowOperator`：

```
   private <R> WindowOperator<K, T, ?, R, W> apply(
            InternalWindowFunction<Iterable<T>, R, K, W> function) {
        if (evictor != null) {
            return buildEvictingWindowOperator(function);
        } else {
            ListStateDescriptor<T> stateDesc =
                    new ListStateDescriptor<>(
                            WINDOW_STATE_NAME, inputType.createSerializer(config));

            return buildWindowOperator(stateDesc, function);
        }
    }
```
可以看到这里创建了`ListStateDescriptor`。

然后，在执行Window的生命周期方法`org.apache.flink.streaming.runtime.operators.windowing.WindowOperator#open()`，Flink 会为每个 key + window 分配一个内部缓存state：

```java
// create (or restore) the state that hold the actual window contents
// NOTE - the state may be null in the case of the overriding evicting window operator
if (windowStateDescriptor != null) {
    windowState =
            (InternalAppendingState<K, W, IN, ACC, ACC>)
                    getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
}

 public <N, S extends State, V> S getOrCreateKeyedState(TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor) throws Exception {
        Preconditions.checkNotNull(namespaceSerializer, "Namespace serializer");
        Preconditions.checkNotNull(this.keySerializer, "State key serializer has not been configured in the config. This operation cannot use partitioned state.");
        InternalKvState<K, ?, ?> kvState = (InternalKvState)this.keyValueStatesByName.get(stateDescriptor.getName());
        if (kvState == null) {
            if (!stateDescriptor.isSerializerInitialized()) {
                stateDescriptor.initializeSerializerUnlessSet(this.executionConfig);
            }

            kvState = LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled((InternalKvState)TtlStateFactory.createStateAndWrapWithTtlIfEnabled(namespaceSerializer, stateDescriptor, this, this.ttlTimeProvider), stateDescriptor, this.latencyTrackingStateConfig);
            this.keyValueStatesByName.put(stateDescriptor.getName(), kvState);
            this.publishQueryableStateIfEnabled(stateDescriptor, kvState);
        }

        return kvState;
    }
```
这里的 `windowState` 就是 Flink 为你隐藏起来的“窗口元素缓存”：  
- `windowState` 存放当前 window 的所有元素 
- Flink 会在每次触发窗口（watermark 到达或触发器触发）时，把这个`windowState` 内容取出来，交给你的 `process()` 方法  
- 当窗口关闭后，会 `clear()`这个`windowState`   

### 2. 数据到达：write to state

在 `WindowOperator#processElement()` 中，Flink 收到新的事件时，会执行：

```java
// 把元素追加到 windowState
windowState.setCurrentNamespace(window);
windowState.add(element.getValue());
……
//触发器触发，则获取窗口状态内容
TriggerResult triggerResult = triggerContext.onElement(element);

if (triggerResult.isFire()) {
    ACC contents = windowState.get();
    if (contents == null) {
        continue;
    }
    emitWindowContents(window, contents);
    
   if (triggerResult.isPurge()) {
                windowState.clear();
            }
            registerCleanupTimer(window);
}

```

这段代码每来一条事件，就把它 **序列化** 并写入到 StateBackend（Memory/RocksDB），而不是保存在 Java内存对象里。

同时，如果是触发了触发器，则会返回窗口内容。还会创建一个定时器，定时执行窗口计算。

### 3. 定时触发：read from state

当定时器触发时，`WindowOperator#onEventTime()` 会调用：

```java
TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
//触发器触发
if (triggerResult.isFire()) {
    // 读取并反序列化所有元素
    ACC contents = windowState.get();
    if (contents != null) {
        emitWindowContents(triggerContext.window, contents);
    }
}

// 清空 state
if (triggerResult.isPurge()) {
    windowState.clear();
}                          
```

从 **写**（`add()`）到 **读**（`get()`）再到 **删**（`clear()`），整个过程都是通过 Flink 的 StateBackend 完成的——这保证了：

1. **Exactly-once 语义**：即使 TaskManager 宕机或网络抖动，StateBackend 里缓存的窗口数据都能在恢复后自动重新加载。  
2. **水平扩容/重分区**：当做 rescale 操作或 job 重启时，Flink 会把 state 分片迁移到新的并行度实例，保证窗口数据完整。  

### 为什么 Window 必须序列化？

1. **分布式容错**  
   Window 算子可能在多个 TM 上并行，节点故障、网络分区、作业重调度都可能发生，只有把窗口数据写到 StateBackend，才能在恢复时不丢失任何事件。

2. **Checkpoint & Savepoint**  
   Flink 借助 Kafka、文件系统做 checkpoint/savepoint；所有 state（包括窗口元素）都会被打包到 checkpoint 里，保证 Exactly-once 与故障恢复。

3. **弹性伸缩**  
   扩容、缩容时需要重新分配并行任务，StateBackend 会把各个 key + window 的数据按照新的并行度迁移到对应实例。

### 总结
源码分析完了，写个小总结吧

- **本地变量** 只能在当前算子实例、当前方法调用中生存，不会参与序列化；重启或缩容后会丢失。  
- **Managed State**（包括我们手动声明的 `ValueState`、也包括 WindowOperator 背后隐式的 `ListState`）会被 Flink 序列化到 StateBackend，参与 checkpoint/savepoint、支持容错恢复和重分区。  
- 虽然 Window API 没让你在代码里 `getState()`，但其核心实现却在算子初始化时**自动注册了 ListStateDescriptor**，并在 `processElement()` / `onTimer()` 里读写、清理这个 state。

了解了这套机制，你就能：  
1. 在自定义算子里**灵活选择**到底用本地变量还是 Managed State；  
2. 明白为什么 Window API 自带的“隐式状态”一定要序列化到后端，以及如何通过 StateBackend 配置（Memory vs RocksDB）来优化性能 