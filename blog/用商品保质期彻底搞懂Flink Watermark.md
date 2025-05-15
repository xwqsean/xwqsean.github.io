---
title: 用商品保质期彻底搞懂Flink Watermark
authors: [强哥叨逼叨]
tags: [flink]
date: 2025-04-11
---

### 为什么要理解 Watermark？
在流处理（Stream Processing）中，数据通常是乱序到达的，也就是说，一个时间戳更早的事件，可能会比一个时间戳更晚的事件更迟到达。

那么问题来了：
- 我们怎么知道某个时间段的数据已经完整，可以进行计算？
- 我们如何处理迟到的数据？

这就需要用到Watermark（水位线）机制。可是，初学者在学习Watermark的时候，总是非常的不好理解，到底Watermark什么时候触发计算？而计算的时候，会计算哪些数据？

那么，接下来强哥就举个比较容易理解的例子，让我们快速记住Watermark的机制。


### 用超市的“商品保质期”来理解 Watermark
我们假设自己是超市的商品管理员，每天需要检查商品的生产日期，决定哪些商品可以上架销售，哪些商品需要下架。

#### 规则
1. 每个商品都有一个生产日期（相当于事件的 `event_time`）。
2. 商品的保质期是 10 天，超过 10 天就必须丢弃（相当于 Watermark）。
3. 我们每天都会检查所有商品，并决定哪些商品可以销售，哪些商品应该下架。


### 例子：商品检查流程
假设今天是 2025-03-15 00:00:00，Watermark 设置为10 天前（即 2025-03-05 00:00:00）。  
我们收到以下商品数据：

| 商品编号 | 生产日期（时间戳）         | 生产日期（转换后）  | 
|---------|-----------------|----------------|
| p1      | 1741795200000   | 2025-03-13 00:00:00 | 
| p2      | 1741622400000   | 2025-03-11 00:00:00 |
| p3      | 1740758400000   | 2025-03-01 00:00:00 |
| p4      | 1741017600000   | 2025-03-04 00:00:00 | 
| p5      | 1741104000000   | 2025-03-05 00:00:00 | 


### Flink 代码示例
我们用Flink Java来实现这个逻辑：

#### 代码功能
- 监听Socket 端口（localhost:9999），实时接收商品数据  
- 设置Watermark（允许 10 天的乱序，自动丢弃过期商品  
- 使用1 天窗口，统计保质期内的商品  
- 把过期商品放入侧输出流，并打印到控制台  

```java
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

public class FlinkWatermarkExpiryCheck {

    // 商品数据结构
    public static class Product {
        public String productName;
        public long productionTimestamp;  // 生产时间戳（毫秒）

        public Product(String productName, long productionTimestamp) {
            this.productName = productName;
            this.productionTimestamp = productionTimestamp;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Socket 输入流（监听 localhost:9999）
        DataStream<String> socketStream = env.socketTextStream("localhost", 9999);

        // 侧输出流：存放过期商品
        OutputTag<Product> expiredTag = new OutputTag<Product>("expired") {};

        // 处理输入数据
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

        // 处理过期商品（从侧输出流获取）
        DataStream<Product> expiredProducts = resultStream.getSideOutput(expiredTag);
        expiredProducts.process(new ProcessFunction<Product, String>() {
            @Override
            public void processElement(Product product, Context ctx, Collector<String> out) throws Exception {
                String formattedTime = DateFormatUtils.format(product.productionTimestamp, "yyyy-MM-dd HH:mm:ss");
                aliveProducts.remove(product.productName);
                System.out.println(String.format(
                        "🚫 过期商品: %s | 生产日期: %s (已超过 10 天)",
                        product.productName, formattedTime
                ));
            }
        });

        // 输出有效商品
        resultStream.print();

        env.execute("Flink Watermark Expiry Check");
    }

    private static List<String> aliveProducts = Lists.newArrayList("p1", "p2", "p3", "p4", "p5");


    // 处理窗口计算，输出当前窗口的时间信息
    public static class ExpiryCheckProcessWindowFunction
            extends ProcessWindowFunction<Product, String, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Product> elements,
                            Collector<String> out) {
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            long currentWatermark = context.currentWatermark();
            long systemTime = System.currentTimeMillis();

            // 格式化时间
            String windowStartTime = DateFormatUtils.format(windowStart, "yyyy-MM-dd HH:mm:ss");
            String windowEndTime = DateFormatUtils.format(windowEnd, "yyyy-MM-dd HH:mm:ss");
            String watermarkTime = DateFormatUtils.format(currentWatermark, "yyyy-MM-dd HH:mm:ss");
            String systemTimeFormatted = DateFormatUtils.format(systemTime, "yyyy-MM-dd HH:mm:ss");

            // 输出窗口信息
            System.out.println(String.format(
                    "✅ 商品检查完成 | Watermark: %s | 窗口: [%s ~ %s] | 系统时间: %s",
                    watermarkTime, windowStartTime, windowEndTime, systemTimeFormatted
            ));
            // 统计有效商品
            elements.forEach(product -> {
                System.out.println(String.format(
                        "🚫 过期商品: %s | 生产日期: %s (已超过 10 天)",
                        product.productName, DateFormatUtils.format(product.productionTimestamp, "yyyy-MM-dd HH:mm:ss")
                ));
                aliveProducts.remove(product.productName);
            });



            System.out.println("🛒 有效商品：" + aliveProducts);
        }
    }
}

```

开启nc，启动flink服务后，我们在控制台先输入今天的日期：
```
今天,1741968000000
```
然后开始检查所有商品，在控制台输入：
```
p1,1741795200000
p2,1741622400000
p3,1740758400000
p4,1741017600000
p5,1741104000000
```
会看到，flink端打印输出：
> 🚫 过期商品: p3 | 生产日期: 2025-03-01 00:00:00 (已超过 10 天)<br></br>
> 🚫 过期商品: p4 | 生产日期: 2025-03-04 00:00:00 (已超过 10 天)

也就是说，今天检查到了p3和p4两个商品已经过期了。

时间到了第二天，我们在控制台输入：
```
第二天,1742054400000
```
会看到，flink端打印输出：
> ✅ 商品检查完成 | Watermark: 2025-03-05 23:59:59 | 窗口: [2025-03-04 08:00:00 ~ 2025-03-05 08:00:00] | 系统时间: 2025-04-03 18:01:34<br></br>
🚫 过期商品: p5 | 生产日期: 2025-03-05 00:00:00 (已超过 10 天)<br></br>
🛒 有效商品：[p1, p2]

商品p5在第二天过期了，为什么呢？我们看到，商品p5的生产日期是2025-03-05 00:00:00，而今天是：2025-03-16 00:00:00，也就是说：2025-03-06 00:00:00之后的商品才是没过期的。我们看到Watermark: 2025-03-05 23:59:59，就是代表只要小于等于2025-03-05 23:59:59的商品就是过期了，所以p5过期了。

时间到了3月21日，我们在控制台输入：
```
3月21日,1742486400000
```
发现flink端没有输出

也就是说没有商品过期，因为今天，Watermark: 2025-03-10 23:59:59，没有在这个之前的商品，最老的商品是p2:2025-03-11 00:00:00。所以没有商品过期。

时间到了3月22日，我们在控制台输入：
```
3月22日,1742572800000
```
会看到，flink端打印输出：
>✅ 商品检查完成 | Watermark: 2025-03-11 23:59:59 | 窗口: [2025-03-10 08:00:00 ~ 2025-03-11 08:00:00] | 系统时间: 2025-04-03 18:26:01<br></br>
🚫 过期商品: p2 | 生产日期: 2025-03-11 00:00:00 (已超过 10 天)<br></br>
🛒 有效商品：[p1]

商品p2过期了，为什么呢？因为商品p2的生产日期是2025-03-11 00:00:00，而今天是：2025-03-22 00:00:00，也就是说：2025-03-12 00:00:00之后的商品才是没过期的。我们看到Watermark: 2025-03-11 23:59:59，就是代表只要小于等于2025-03-11 23:59:59的商品就是过期了，所以p2过期了。

### 水位线与窗口的关系
通过学习了以上例子后，我们再用文字描述总结可能就能更好的理解了。

水位线与窗口的关系：
- 触发计算：水位线的时间戳用于触发窗口的关闭和计算。当水位线的时间戳超过窗口的结束时间时，窗口会关闭并开始计算。
- 处理迟到数据：水位线可以帮助窗口处理迟到数据。迟到数据是指在窗口关闭后到达的数据。通过设置水位线的延迟，可以允许一定时间内的迟到数据被窗口收集并处理。
- 独立性：虽然水位线和窗口紧密配合，但它们是独立的机制。水位线的生成基于事件时间和延迟，而窗口的计算基于窗口的定义和水位线的触发。

最后再看一个示例：
假设有一个流处理任务，需要对每10秒的数据进行聚合计算。具体步骤如下：

设置水位线：假设水位线的延迟为2秒。

创建窗口：定义一个滚动窗口，窗口大小为10秒。

数据到达：
```
数据1（时间戳：12:00:00）到达，水位线设置为12:00:00 - 2秒 = 11:59:58。

数据2（时间戳：12:00:05）到达，水位线更新为12:00:05 - 2秒 = 12:00:03。

数据3（时间戳：12:00:12）到达，水位线更新为12:00:12 - 2秒 = 12:00:10。
```
触发计算：当水位线的时间戳达到12:00:10时，触发12:00:00 - 12:00:10窗口的计算。

处理迟到数据：如果在水位线为12:00:10之后，又来了一个时间戳为12:00:04的数据，该数据会被丢弃或放入下一个窗口，具体取决于系统的配置。
通过水位线和窗口的配合，流处理系统可以在处理乱序数据的同时，保证计算的正确性和效率。

好了，通过上面的内容，是否理解了flink的水位线与窗口了呢？那强哥再问个问题：
> 超市的例子，控制台输入多少，最后的一个商品才会过期呢？