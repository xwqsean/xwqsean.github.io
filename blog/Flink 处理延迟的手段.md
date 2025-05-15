---
title: Flink 处理延迟的手段
authors: [强哥叨逼叨]
tags: [flink]
date: 2025-04-01
---

### 牢记
`watermark`相关的文章写了几篇，今天先做个小节吧。  
- `watermark`是针对事件时间的，主要的内容是一个时间戳，用来表示当前事件时间的进展
- `watermark`解决的是数据延迟问题
- `watermark(t)`，表示在当前流中事件时间已经达到了时间戳t，这代表t之前的所有数据都到齐了，之后流中不会出现时间戳t’ ≤ t的数据。也就是说`watermark`的时间戳必须单调递增，以确保任务的事件时间时钟一直向前推进
- `watermark`不解决的窗口计算时的数据顺序
- `watermark`其实可以简单的理解为触发窗口计算的卡口。在没有设置`watermark`的时候，窗口：[窗口开始时间,窗口结束时间)是在处理时间到达窗口结束时间时触发。而设置了`watermark`后，触发窗口的计算时间就是：窗口结束时间+`watermark`延迟的时间。只是在flink的实现中，要注意到`onEvent`和`onPeriodicEmit`两个方法。

### 回顾
上篇文章其实最后的一个例子写完后，时间太晚了，没来的及多解释下，我们再简单回顾下
```
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
我们自己实现了`onEvent`和`onPeriodicEmit`方法。其实，有看过`forBoundedOutOfOrderness`源码的小伙伴应该会有些熟悉，因为我们自己实现的`MyWatermarkGenerator`基本上和它的实现差不多。`forBoundedOutOfOrderness`的源码是这样的：
```
@Public
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

    /** The maximum timestamp encountered so far. */
    private long maxTimestamp;

    /** The maximum out-of-orderness that this watermark generator assumes. */
    private final long outOfOrdernessMillis;

    /**
     * Creates a new watermark generator with the given out-of-orderness bound.
     *
     * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
     */
    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    // ------------------------------------------------------------------------

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }
}
```
`onEvent`方法是每条数据来的时候都会执行，在代码中可以看到，这里这是设置了`maxTimestamp`，并没有触发`watermark`更新。而要触发`watermark`更新，需要调用：`output.emitWatermark`方法。

`onPeriodicEmit`则是flink周期性触发执行的，默认200ms一次。我们看到，在它的实现里面，就触发了`watermark`更新。如果想修改默认周期时间，可以通过下面方法修改。例如：修改为400ms。
```
env.getConfig().setAutoWatermarkInterval(400L);
```

我们知道`BoundedOutOfOrdernessWatermarks`是`WatermarkStrategy`乱序流中内置水位线的设置。而另外一个`forMonotonousTimestamps`有序流中内置水位线的设置。其实，从源码来看，`forMonotonousTimestamps`内部的实现最后还是走了`BoundedOutOfOrdernessWatermarks`：
```
static <T> WatermarkStrategy<T> forMonotonousTimestamps() {
    return (ctx) -> new AscendingTimestampsWatermarks<>();
}

@Public
public class AscendingTimestampsWatermarks<T> extends BoundedOutOfOrdernessWatermarks<T> {

    /** Creates a new watermark generator with for ascending timestamps. */
    public AscendingTimestampsWatermarks() {
        super(Duration.ofMillis(0));
    }
}
```
所以，其实理解了`BoundedOutOfOrdernessWatermarks`乱序流中内置水位线的设置就算是理解了水位线啦。

### 迟到的数据
从什么的`watermark`的设置中，我们其实可以看到，`watermark`只是做了延迟窗口计算的触发，但是当窗口开始计算后，还有延迟的数据，就没法再进入窗口计算了，还是可能存在数据丢失的情况。而如果我们的数据非常重要，不能延迟该怎么办呢？

flink还提供了两个手段处理这些延迟数据：
- Allow Lateness 窗口已经开始计算时，允许延迟一段时间后再关闭窗口。
- Side Output对真正延迟的数据，统一放入到另一条输出流中。有种兜底处理的意思。
代码中这么使用就好了，这块就不多描述了。兜底手段，记得有这个就行。
```
OutputTag<Product> expiredTag = new OutputTag<Product>("expired") {};
socketStream
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
  .allowedLateness(Time.seconds(2)) // 推迟2s关窗
  .sideOutputLateData(expiredTag)  // 过期商品进入侧输出流
  .process(new ExpiryCheckProcessWindowFunction());  // 窗口计算逻辑
```
好啦，`watermark`相关的知识就这些啦。`watermark`完结，撒花~