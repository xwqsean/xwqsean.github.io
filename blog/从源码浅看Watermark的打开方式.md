---
title: 从源码浅看Watermark的打开方式
authors: [强哥叨逼叨]
tags: [flink]
date: 2025-04-16
---

## 赌场失意
老实说，就今天赌场跌成这样，还能有心情给大家分享，甚至早上还去抄了底，此刻大半夜心情毫无波澜的写东西，你们就该好好的拿椅子坐下学习。

不过，我也知道关注我的人里面，很多其实不是学后端的，大数据更别扯蛋了，不过发了推文，没取关的，都是yifu，所以也尽量有的时候分享些别的东西，写文章虽然是想要记录自己的学习和生活，不过既然写了，当然也希望看的人不会觉得太无趣。

## `watermark`
在`Flink DataStream`中流动着不同的元素，统称为`StreamElement`，`StreamElement`可以是`StreamRecord`、`Watermark`、`StreamStatus`、`LatencyMarker`中任何一种类型。

**`StreamElement`**  
`StreamElement`是一个抽象类(是Flink 承载消息的基类)，其他四种类型继承`StreamElement`。
```
public abstract class StreamElement {
  //判断是否是Watermark
  public final boolean isWatermark() {
    return getClass() == Watermark.class;
  }
  //判断是否为StreamStatus
  public final boolean isStreamStatus() {
    return getClass() == StreamStatus.class;
  }
  //判断是否为StreamRecord
  public final boolean isRecord() {
    return getClass() == StreamRecord.class;
  }
  //判断是否为LatencyMarker
  public final boolean isLatencyMarker() {
    return getClass() == LatencyMarker.class;
  }
  //转换为StreamRecord
  public final <E> StreamRecord<E> asRecord() {
    return (StreamRecord<E>) this;
  }
  //转换为Watermark
  public final Watermark asWatermark() {
    return (Watermark) this;
  }
  //转换为StreamStatus
  public final StreamStatus asStreamStatus() {
    return (StreamStatus) this;
  }
  //转换为LatencyMarker
  public final LatencyMarker asLatencyMarker() {
    return (LatencyMarker) this;
  }
}

```
`**Watermark**`  
`Watermark`继承了`StreamElement`。`Watermark` 是和事件一个级别的抽象，其内部包含一个成员变量时间戳`timestamp`，标识当前数据的时间进度。`Watermark`实际上作为数据流的一部分随数据流流动。
```
@PublicEvolving
public final class Watermark extends StreamElement {
  /*The watermark that signifies end-of-event-time. */
  public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);
  /* The timestamp of the watermark in milliseconds. */
  private final long timestamp;
  /* Creates a new watermark with the given timestamp in milliseconds.*/
  public Watermarklong timestamp) {
	this.timestamp = timestamp;
  }
  /*Returns the timestamp associated with this {@link Watermark} in milliseconds.**/
  public long getTimestamp() {
    return timestamp;
  }
}

```

## 回顾下
上周我们分享了`flink watermark`，用商品过期时间来类比`watermark`的使用场景。我们来看看我们当时`watermark`是怎么使用的还记得吗？
```
SingleOutputStreamOperator<String> resultStream = socketStream
                .map((MapFunction<String, Product>) value -> {
                    String[] parts = value.split(",");
                    String name = parts[0];
                    long timestamp = Long.parseLong(parts[1]);
                    return new Product(name, timestamp);
                })
                // 指定 Watermark 生成策略（最大允许 10 天的乱序）
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Product>forBoundedOutOfOrderness(Duration.ofDays(10))
                                .withTimestampAssigner((event, timestamp) -> event.productionTimestamp)
                )
                .keyBy(product -> product.productName)  // 按商品名分组
                .window(TumblingEventTimeWindows.of(Time.days(1))) // 窗口大小 1 天
                .sideOutputLateData(expiredTag)  // 过期商品进入侧输出流
                .process(new ExpiryCheckProcessWindowFunction());  // 窗口计算逻辑
```
一大段用了ramda表达式写在了一起，我们就挑设置`watermark`部分看：
```
.assignTimestampsAndWatermarks(
  WatermarkStrategy
  .<Product>forBoundedOutOfOrderness(Duration.ofDays(10))
  .withTimestampAssigner((event, timestamp) -> event.productionTimestamp)
)
```
这里调用了`WatermarkStrategy`的`forBoundedOutOfOrderness`和`withTimestampAssigner`。通过`WatermarkStrategy`就设置好了`watermark`，那我们就来看看`WatermarkStrategy`是干嘛的。

## WatermarkStrategy
`WatermarkStrategy`从调用来看，像是个类，可是看源码，其实它是个接口，里面定义了许多default方法。来看看接口的备注：
> The WatermarkStrategy defines how to generate Watermarks in the stream sources. The WatermarkStrategy is a builder/factory for the WatermarkGenerator that generates the watermarks and the TimestampAssigner which assigns the internal timestamp of a record.  
This interface is split into three parts: 1) methods that an implementor of this interface needs to implement, 2) builder methods for building a WatermarkStrategy on a base strategy, 3) convenience methods for constructing a WatermarkStrategy for common built-in strategies or based on a WatermarkGeneratorSupplier

内容说的很清楚，`WatermarkStrategy`是个接口也是工厂，可以用`WatermarkGenerator`生成`Watermarks`，还可以用`TimestampAssigner`来设置数据记录的内部时间戳。接口的两个非default无具体实现的方法：`createWatermarkGenerator`和`createTimestampAssigner`就是做这两件事用的。

而除此之外的其他有具体实现以default方法或者static静态实现分为两部分，default方法是在基础的`strategy`之上通过builder模式构建`WatermarkStrategy`的；static静态实现一种是利用已经内置好的常见`strategy`，这些常见`strategy`是预先定义好的，可以直接拿来使用，就像使用现成的工具一样方便；另一种是基于`WatermarkGeneratorSupplier`来构造，通过它可以根据具体需求来生成相应的`watermarkStrategy`，这种方式更具灵活性，可以根据不同的场景和要求来定制。
```
@Public
public interface WatermarkStrategy<T>
        extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T> {

    // ------------------------------------------------------------------------
    //  Methods that implementors need to implement.
    // ------------------------------------------------------------------------

    /** Instantiates a WatermarkGenerator that generates watermarks according to this strategy. */
    @Override
    WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);

    /**
     * Instantiates a {@link TimestampAssigner} for assigning timestamps according to this strategy.
     */
    @Override
    default TimestampAssigner<T> createTimestampAssigner(
            TimestampAssignerSupplier.Context context) {
        // By default, this is {@link RecordTimestampAssigner},
        // for cases where records come out of a source with valid timestamps, for example from
        // Kafka.
        return new RecordTimestampAssigner<>();
    }

    /**
     * Provides configuration for watermark alignment of a maximum watermark of multiple
     * sources/tasks/partitions in the same watermark group. The group may contain completely
     * independent sources (e.g. File and Kafka).
     *
     * <p>Once configured Flink will "pause" consuming from a source/task/partition that is ahead of
     * the emitted watermark in the group by more than the maxAllowedWatermarkDrift.
     */
    @PublicEvolving
    default WatermarkAlignmentParams getAlignmentParameters() {
        return WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED;
    }

    // ------------------------------------------------------------------------
    //  Builder methods for enriching a base WatermarkStrategy
    // ------------------------------------------------------------------------

    /**
     * Creates a new {@code WatermarkStrategy} that wraps this strategy but instead uses the given
     * {@link TimestampAssigner} (via a {@link TimestampAssignerSupplier}).
     *
     * <p>You can use this when a {@link TimestampAssigner} needs additional context, for example
     * access to the metrics system.
     *
     * <pre>
     * {@code WatermarkStrategy<Object> wmStrategy = WatermarkStrategy
     *   .forMonotonousTimestamps()
     *   .withTimestampAssigner((ctx) -> new MetricsReportingAssigner(ctx));
     * }</pre>
     */
    default WatermarkStrategy<T> withTimestampAssigner(
            TimestampAssignerSupplier<T> timestampAssigner) {
        checkNotNull(timestampAssigner, "timestampAssigner");
        return new WatermarkStrategyWithTimestampAssigner<>(this, timestampAssigner);
    }

    /**
     * Creates a new {@code WatermarkStrategy} that wraps this strategy but instead uses the given
     * {@link SerializableTimestampAssigner}.
     *
     * <p>You can use this in case you want to specify a {@link TimestampAssigner} via a lambda
     * function.
     *
     * <pre>
     * {@code WatermarkStrategy<CustomObject> wmStrategy = WatermarkStrategy
     *   .<CustomObject>forMonotonousTimestamps()
     *   .withTimestampAssigner((event, timestamp) -> event.getTimestamp());
     * }</pre>
     */
    default WatermarkStrategy<T> withTimestampAssigner(
            SerializableTimestampAssigner<T> timestampAssigner) {
        checkNotNull(timestampAssigner, "timestampAssigner");
        return new WatermarkStrategyWithTimestampAssigner<>(
                this, TimestampAssignerSupplier.of(timestampAssigner));
    }

    /**
     * Creates a new enriched {@link WatermarkStrategy} that also does idleness detection in the
     * created {@link WatermarkGenerator}.
     *
     * <p>Add an idle timeout to the watermark strategy. If no records flow in a partition of a
     * stream for that amount of time, then that partition is considered "idle" and will not hold
     * back the progress of watermarks in downstream operators.
     *
     * <p>Idleness can be important if some partitions have little data and might not have events
     * during some periods. Without idleness, these streams can stall the overall event time
     * progress of the application.
     */
    default WatermarkStrategy<T> withIdleness(Duration idleTimeout) {
        checkNotNull(idleTimeout, "idleTimeout");
        checkArgument(
                !(idleTimeout.isZero() || idleTimeout.isNegative()),
                "idleTimeout must be greater than zero");
        return new WatermarkStrategyWithIdleness<>(this, idleTimeout);
    }

    /**
     * Creates a new {@link WatermarkStrategy} that configures the maximum watermark drift from
     * other sources/tasks/partitions in the same watermark group. The group may contain completely
     * independent sources (e.g. File and Kafka).
     *
     * <p>Once configured Flink will "pause" consuming from a source/task/partition that is ahead of
     * the emitted watermark in the group by more than the maxAllowedWatermarkDrift.
     *
     * @param watermarkGroup A group of sources to align watermarks
     * @param maxAllowedWatermarkDrift Maximal drift, before we pause consuming from the
     *     source/task/partition
     */
    @PublicEvolving
    default WatermarkStrategy<T> withWatermarkAlignment(
            String watermarkGroup, Duration maxAllowedWatermarkDrift) {
        return withWatermarkAlignment(
                watermarkGroup,
                maxAllowedWatermarkDrift,
                WatermarksWithWatermarkAlignment.DEFAULT_UPDATE_INTERVAL);
    }

    /**
     * Creates a new {@link WatermarkStrategy} that configures the maximum watermark drift from
     * other sources/tasks/partitions in the same watermark group. The group may contain completely
     * independent sources (e.g. File and Kafka).
     *
     * <p>Once configured Flink will "pause" consuming from a source/task/partition that is ahead of
     * the emitted watermark in the group by more than the maxAllowedWatermarkDrift.
     *
     * @param watermarkGroup A group of sources to align watermarks
     * @param maxAllowedWatermarkDrift Maximal drift, before we pause consuming from the
     *     source/task/partition
     * @param updateInterval How often tasks should notify coordinator about the current watermark
     *     and how often the coordinator should announce the maximal aligned watermark.
     */
    @PublicEvolving
    default WatermarkStrategy<T> withWatermarkAlignment(
            String watermarkGroup, Duration maxAllowedWatermarkDrift, Duration updateInterval) {
        return new WatermarksWithWatermarkAlignment<T>(
                this, watermarkGroup, maxAllowedWatermarkDrift, updateInterval);
    }

    // ------------------------------------------------------------------------
    //  Convenience methods for common watermark strategies
    // ------------------------------------------------------------------------

    /**
     * Creates a watermark strategy for situations with monotonously ascending timestamps.
     *
     * <p>The watermarks are generated periodically and tightly follow the latest timestamp in the
     * data. The delay introduced by this strategy is mainly the periodic interval in which the
     * watermarks are generated.
     *
     * @see AscendingTimestampsWatermarks
     */
    static <T> WatermarkStrategy<T> forMonotonousTimestamps() {
        return (ctx) -> new AscendingTimestampsWatermarks<>();
    }

    /**
     * Creates a watermark strategy for situations where records are out of order, but you can place
     * an upper bound on how far the events are out of order. An out-of-order bound B means that
     * once the an event with timestamp T was encountered, no events older than {@code T - B} will
     * follow any more.
     *
     * <p>The watermarks are generated periodically. The delay introduced by this watermark strategy
     * is the periodic interval length, plus the out of orderness bound.
     *
     * @see BoundedOutOfOrdernessWatermarks
     */
    static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
        return (ctx) -> new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness);
    }

    /** Creates a watermark strategy based on an existing {@link WatermarkGeneratorSupplier}. */
    static <T> WatermarkStrategy<T> forGenerator(WatermarkGeneratorSupplier<T> generatorSupplier) {
        return generatorSupplier::createWatermarkGenerator;
    }

    /**
     * Creates a watermark strategy that generates no watermarks at all. This may be useful in
     * scenarios that do pure processing-time based stream processing.
     */
    static <T> WatermarkStrategy<T> noWatermarks() {
        return (ctx) -> new NoWatermarksGenerator<>();
    }
}

```
其实不是很想贴这么长段的源码进来，可是这个类又比较重要。所以各位担待一下。  

这里也要注意一点，`watermark`其实就是处理基于**事件时间**的触发时机的关键机制，但它比事件时间多了一个含义：乱序处理 + 推进触发计算。同时，`watermark`主要处理的是**延迟**而不是**排序**。**它能帮助窗口在等待一定时间后触发计算，尽可能覆盖乱序数据，但不会保证窗口内的事件处理顺序，需要你自己排序处理。。**

我们在日常使用的时候，其实只要和上面的例子中那样，用`withTimestampAssigner`告诉flink使用数据的哪个字段作为事件时间，以及使用一个内置的实现类比如`forBoundedOutOfOrderness`告诉flink，为存在乱序事件但可确定最大无序时间边界的场景创建水位线生成策略就已经足够了。一般要自己调用`forGenerator`方法来创建`WatermarkStrategy`的情况比较少。

## 用`forGenerator`自己实现一个
既然我们是在学习，那么要快速的熟悉一个东西该怎么使用，无疑是自己实现一个上手和了解的更全面。所以，接下来，我们就用`forGenerator(WatermarkGeneratorSupplier<T> generatorSupplier) `来构造一个`WatermarkStrategy`。

下面，我们以常见的词频统计作为示例，展现一下 Watermark 的实现代码：
```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 设置 Watermark 生成周期为 2s
env.getConfig().setAutoWatermarkInterval(2000);

// 输入数据格式：时间戳，单词，频次
// 例如：1000,a,1
DataStreamSource<String> source = env.socketTextStream("192.168.117.128", 9999);

// 定义 Watermark 策略
WatermarkStrategy<String> strategy =
        WatermarkStrategy.<String>forGenerator(new MyWatermarkGeneratorSupplier<>(Duration.ofSeconds(3)))
                .withTimestampAssigner(context -> (s, l) -> Long.parseLong(s.split(",")[0]));

// 业务处理：transformation
source
        .assignTimestampsAndWatermarks(strategy)
        .map(value -> {
            String[] splits = value.split(",");
            return Tuple2.of(splits[1].trim(), Integer.parseInt(splits[2].trim()));
        })
        .returns(Types.TUPLE(Types.STRING, Types.INT))
        .keyBy(value -> value.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce((Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) -> {
            System.out.println("-----reduce invoked----" + value1.f0 + "==>" + (value1.f1 + value2.f1));
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
        }, new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {

            @Override
            public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) {

                FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

                for (Tuple2<String, Integer> element : iterable) {
                    collector.collect("[" + format.format(context.window().getStart()) + "==>" +
                            format.format(context.window().getEnd()) + "], " + element.f0 + "==>" + element.f1);
                }
            }
        })
        .print();

env.execute("Watermark App");
```
在本例中，为了方便 Watermark 的打印，我们实现了自定义的 WatermarkGenerator，其代码如下所示：
```
public class MyWatermarkGeneratorSupplier<T> implements WatermarkGeneratorSupplier<T> {

    private final Duration duration;

    public MyWatermarkGeneratorSupplier(Duration duration) {
        this.duration = duration;
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(Context context) {
        return new MyWatermarkGenerator<>(duration);
    }
}

public class MyWatermarkGenerator<T> implements WatermarkGenerator<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyWatermarkGenerator.class);

    private long maxTimestamp;
    private final long outOfOrdernessMillis;

    public MyWatermarkGenerator(Duration maxOutOfOrderness) {
        Preconditions.checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        Preconditions.checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        this.maxTimestamp = -9223372036854775808L + this.outOfOrdernessMillis + 1L;
    }

    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.maxTimestamp = Math.max(this.maxTimestamp, eventTimestamp);
    }

    public void onPeriodicEmit(WatermarkOutput output) {
        Watermark watermark = new Watermark(this.maxTimestamp - this.outOfOrdernessMillis - 1L);
        output.emitWatermark(watermark);
        LOGGER.info("current watermark ==> {}", watermark.getTimestamp());
    }
}
```
好了，代码比较简单，自己看看就差不多了，今天就到这吧。
