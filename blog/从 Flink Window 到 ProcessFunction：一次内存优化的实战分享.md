---
title: 从 Flink Window 到 ProcessFunction：一次内存优化的实战分享
authors: [强哥叨逼叨]
tags: [flink]
date: 2025-04-30
---

最近在做一个实时计算项目中，需要对用户行为日志进行聚合分析。起初，同事使用了 Flink 的Window API（如 `TumblingEventTimeWindows` + `WindowFunction`）来实现 5 分钟滚动统计。

功能上没问题，但随着数据量的增长，作业频繁触发 GC，内存占用飙升，最终甚至出现了 OOM 错误。经排查后发现：**Window 中的数据缓存策略，是性能瓶颈的关键**。

### 问题分析

Flink 的 Window API 会在后台自动缓存每个 key、每个窗口内的所有元素：

- 比如一个 5 分钟窗口，每个用户行为事件都需要被保留在 Flink 的 window state 中；
- 对于高 QPS 场景，每秒成千上万条事件都要被保存；
- 随着 key 数量增加（如按用户、页面等分组），缓存压力线性增长。

> 本质问题：Window 是“批处理”思想的落地，而我们这个需求更适合“事件驱动”处理。

### 问题优化

为了解决这个问题，我尝试用更轻量的方式重构逻辑 —— 替换 Window，为每个 key 维护一个简单的状态 + 定时器，模拟滚动窗口的功能。

#### 重构后的思路：

- 每来一条数据，立即更新一个 `ValueState`（如 count 或 sum）；
- 注册一个定时器（如 `ctx.timerService().registerEventTimeTimer(...)`）；
- 到时间触发处理并清理状态（及时释放内存）；
- 无需保存每条事件，**只保存必要的中间值（例如总和、计数等）**。

#### 示例代码片段

```java
public class RollingCountProcessFunction extends KeyedProcessFunction<String, Event, Result> {

    private transient ValueState<Long> countState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor =
            new ValueStateDescriptor<>("count", Long.class);
        countState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Event value, Context ctx, Collector<Result> out) throws Exception {
        Long current = Optional.ofNullable(countState.value()).orElse(0L);
        countState.update(current + 1);
        ctx.timerService().registerEventTimeTimer(value.getEventTime() + 5 * 60 * 1000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Result> out) throws Exception {
        Long count = countState.value();
        out.collect(new Result(ctx.getCurrentKey(), count));
        countState.clear(); // 清理状态，释放内存
    }
}
```

### 性能对比

| 指标              | 使用 Window API       | 使用 ProcessFunction      |
|-------------------|-----------------------|----------------------------|
| 内存占用          | 高，频繁 GC           | 显著下降，状态更精简       |
| 延迟              | 固定窗口延迟触发       | 实时响应，低延迟            |
| 灵活性            | 逻辑固定              | 高度灵活，自定义状态和行为  |
| 容错与 Checkpoint | 支持               | 同样支持                 |

### 最终效果

在实际部署后，以下指标得到显著优化：

- **TaskManager 内存使用从约 2.5GB 降至 300MB**
- **GC 次数减少 90% 以上**
- **窗口统计延迟下降至毫秒级**

### 小结

在高吞吐、低延迟、内存敏感的场景下，**Window API 可能不是最优解**。它更适合用于“聚合历史数据”，但对于事件驱动类业务，我们可以用 `ProcessFunction` 构建更轻量的替代方案：

- 仅保留必要的中间状态；
- 更快释放内存，避免数据缓存堆积；
- 同时保持灵活性和一致性。

> 小结：Flink 是一个高度灵活的平台，**正确选择 API 的粒度和机制，远比堆配置参数更有效**。

### 官网建议

我们如果有看flink的官方文档，也可以在windows章节看到相关解释和建议，强哥这里直接摘录出来，英文好的可以看看

……引用了半天，公众号每个引用不让超过300字，醉了。那直接给链接大家自己看吧：
> https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/operators/windows/#window-lifecycle

> https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/operators/windows/#window-functions

> https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/operators/windows/#useful-state-size-considerations

是同一页的，但是内容太多，看我发的具体锚点部分就行。

