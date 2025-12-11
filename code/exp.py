import argparse
import logging
import math
import random
import sys
import time
import uuid
import os
from typing import Iterable, Tuple
from datetime import datetime
from pyflink.common import WatermarkStrategy, Duration, Types, Time
from pyflink.common.serialization import Encoder
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, WindowedStream
from pyflink.datastream.functions import MapFunction, AggregateFunction, ProcessWindowFunction, ProcessFunction
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows, TimeWindow
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy, BucketAssigner
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from model import UserEvent, EventAccumulator, WindowAggregateResult, LateEventMetric, CustomTimestampAssigner


class EventGeneratorMapFunction(MapFunction):
    def __init__(self, max_out_of_orderness_ms, late_event_fraction, severe_lateness_upper_bound_ms):
        self.max_out_of_orderness_ms = max_out_of_orderness_ms # 正常乱序的最大时长
        self.late_event_fraction = late_event_fraction         # 迟到数据（超过 Watermark）的比例
        self.severe_lateness_upper_bound_ms = severe_lateness_upper_bound_ms # 严重迟到的上限
        self.event_types = ["click", "view", "purchase", "logout"] # 可选的事件类型

    def map(self, value):
        base_timestamp = int(time.time() * 1000)
        
        # 模拟正常乱序：生成一个 0 到 max_out_of_orderness_ms 之间的随机延迟
        delay = random.randint(0, self.max_out_of_orderness_ms)
        event_timestamp = base_timestamp - delay # 事件时间 = 当前时间 - 延迟

        # 模拟严重迟到数据 (Late Event)：如果随机数命中迟到比例
        if random.random() < self.late_event_fraction:
            # 生成一个额外的延迟，确保时间戳早于 (当前时间 - max_out_of_orderness)，从而被视为迟到
            extra_delay = self.max_out_of_orderness_ms + random.randint(0, self.severe_lateness_upper_bound_ms)
            event_timestamp = base_timestamp - extra_delay
        
        # 随机生成用户 ID (1-100)
        user_id = random.randint(1,100)
        return UserEvent(
            user_id=f"user-{user_id}",
            event_type=random.choice(self.event_types),
            event_time=event_timestamp,
            amount=random.uniform(1.0, 500.0),
            ingestion_time=base_timestamp
        )

class EventAggregateFunctionImpl(AggregateFunction):
    # 创建累加器，每个窗口每个 Key 调用一次
    def create_accumulator(self):
        return EventAccumulator() # 返回 model.py 中定义的累加器实例

    # 将新流入的数据添加到累加器中
    def add(self, value, accumulator):
        accumulator.add(value) 
        return accumulator

    def get_result(self, accumulator):
        return accumulator 

    def merge(self, a, b):
        a.merge(b) 
        return a


class WindowResultProcessFunctionImpl(ProcessWindowFunction):
    def __init__(self, configured_watermark_delay_ms):
        super().__init__()
        self.configured_watermark_delay_ms = configured_watermark_delay_ms # 保存配置的水位线延迟参数


    def process(self, key, context, elements: Iterable[EventAccumulator]):
     
        accumulator = next(iter(elements))
        
        # 获取当前的水位线
        current_watermark = context.current_watermark()
        # 获取窗口的结束时间
        window_end = context.window().end
        # 计算触发滞后时间：如果 Watermark 推进到了 Window End 之后，计算差值
        trigger_lag_ms = max(0, current_watermark - window_end) if current_watermark >= 0 else -1
        # 获取当前系统处理时间
        trigger_system_time = int(time.time() * 1000)

        # 构造最终的窗口结果对象
        result = WindowAggregateResult(
            user_id=key,
            window_start=context.window().start, # 窗口开始时间
            window_end=window_end,               # 窗口结束时间
            configured_watermark_delay_ms=self.configured_watermark_delay_ms,
            current_watermark=current_watermark,
            trigger_lag_ms=trigger_lag_ms,
            trigger_system_time=trigger_system_time,
            event_count=accumulator.count,
            sum_amount=accumulator.sum_amount,
            average_amount=accumulator.get_average_amount(),
            min_amount=accumulator.min_amount,
            max_amount=accumulator.max_amount,
            min_event_time=accumulator.min_event_time,
            max_event_time=accumulator.max_event_time
        )
        yield result

# 定义处理函数：专门用于处理侧输出流中的迟到数据
class LateEventProcessFunctionImpl(ProcessFunction):
    # 处理每一个迟到元素
    def process_element(self, value: UserEvent, ctx):
        # 获取当前定时器服务中的水位线
        current_watermark = ctx.timer_service().current_watermark()
        # 计算迟到了多久 (当前水位线 - 数据自带的事件时间)
        lateness = (current_watermark - value.event_time) if current_watermark >= 0 else -1 # type: ignore
        # 构造并输出迟到指标对象
        yield LateEventMetric(
            user_id=value.user_id,
            event_time=value.event_time,
            detection_time=int(time.time() * 1000),
            lateness_ms=lateness
        )


def run_job(args):
    watermark_delay_ms = args.watermark_delay_ms      # Watermark 生成的最大乱序等待时间
    window_size_ms = args.window_size_ms              # 窗口大小
    window_slide_ms = args.window_slide_ms            # 滑动步长 (用于滑动窗口)
    allowed_lateness_ms = args.allowed_lateness_ms    # 允许迟到时间 (窗口关闭后的宽限期)
    window_type = args.window_type                    # 窗口类型 (tumbling 或 sliding)
    events_per_second = args.events_per_second        # DataGen 生成速率
    run_duration_ms = args.run_duration_ms            # 运行总时长限制
    max_out_of_orderness_ms = args.max_out_of_orderness_ms # 数据生成时的乱序程度
    late_event_fraction = args.late_event_fraction    # 迟到数据比例
    severe_lateness_upper_bound_ms = args.severe_lateness_upper_bound_ms # 严重迟到上限
    parallelism = args.parallelism                    # 并行度
    auto_watermark_interval_ms = args.auto_watermark_interval_ms # Watermark 生成周期
    enable_checkpointing = args.enable_checkpointing  # 是否开启 Checkpoint
    checkpoint_interval_ms = args.checkpoint_interval_ms # Checkpoint 间隔
    output_dir = args.output_dir                      # 输出目录

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    # 设置 Watermark 的自动生成间隔 (周期性调用 Assigner)
    env.get_config().set_auto_watermark_interval(auto_watermark_interval_ms)
    
   
    current_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(current_dir, 'model.py')
    env.add_python_file(model_path)

    t_env = StreamTableEnvironment.create(env)

    if enable_checkpointing:
        env.enable_checkpointing(checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
        env.get_checkpoint_config().set_min_pause_between_checkpoints(int(checkpoint_interval_ms / 2))
        env.get_checkpoint_config().set_checkpoint_timeout(max(checkpoint_interval_ms * 2, 120000))
    

    # 定义 Watermark 策略
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_millis(watermark_delay_ms)) \
        .with_timestamp_assigner(CustomTimestampAssigner())

    # 设置每秒生成的行数
    rows_per_second = events_per_second 
    # 构建 DataGen 连接器的 DDL 选项列表
    ddl_options = [
        f"'connector' = 'datagen'",                   # 使用 datagen 连接器
        f"'rows-per-second' = '{rows_per_second}'",   # 速率
        "'fields.f_sequence.kind' = 'sequence'",      # 字段生成策略：序列
        "'fields.f_sequence.start' = '1'"             # 序列起始值
    ]

    if run_duration_ms > 0:
        total_rows = int(rows_per_second * (run_duration_ms / 1000))
        ddl_options.append(f"'fields.f_sequence.end' = '{total_rows}'")
    else:
        ddl_options.append("'fields.f_sequence.end' = '9223372036854775807'")


    ddl = f"""
        CREATE TEMPORARY TABLE datagen_source (
            f_sequence BIGINT
        ) WITH (
            {", ".join(ddl_options)}
        )
    """
    t_env.execute_sql(ddl)
    

    source_table = t_env.from_path("datagen_source")
    # 将 Table 转换为 DataStream
    raw_stream = t_env.to_data_stream(source_table)
    
    # 对原始流进行 Map 操作：生成模拟的用户行为数据 (UserEvent)
    raw_events = raw_stream \
        .map(EventGeneratorMapFunction(max_out_of_orderness_ms, late_event_fraction, severe_lateness_upper_bound_ms)) \
        .name("synthetic-out-of-order-source") 

    # 为数据流分配时间戳和 Watermark
    events_with_timestamps = raw_events \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .name("assign-watermarks")

    # 定义侧输出标签，用于捕获迟到数据。Types.PICKLED_BYTE_ARRAY() 是 Python 对象的通用序列化格式
    late_output_tag = OutputTag("late-events", Types.PICKLED_BYTE_ARRAY())

    # 按照 user_id 进行 KeyBy 分组
    keyed_stream = events_with_timestamps.key_by(lambda e: e.user_id)

    # 
    # 定义窗口逻辑
    windowed_stream = None
    if "sliding" == window_type.lower():
        
        windowed_stream = keyed_stream.window(SlidingEventTimeWindows.of(
            Time.milliseconds(window_size_ms), 
            Time.milliseconds(window_slide_ms)))
    else:
        windowed_stream = keyed_stream.window(TumblingEventTimeWindows.of(
            Time.milliseconds(window_size_ms)))

    if allowed_lateness_ms > 0:
        # 设置允许迟到时间。在此时间内的迟到数据会再次触发窗口计算
        windowed_stream = windowed_stream.allowed_lateness(allowed_lateness_ms)
    
    # 超过 Allowed Lateness 的数据会被发送到这
    windowed_stream = windowed_stream.side_output_late_data(late_output_tag)

    
    window_results = windowed_stream \
        .aggregate(EventAggregateFunctionImpl(), WindowResultProcessFunctionImpl(watermark_delay_ms)) \
        .name("window-aggregation")

    # 获取侧输出流中的迟到数据
    late_events = window_results.get_side_output(late_output_tag)
    # 对迟到数据进行处理，计算迟到指标
    late_metrics = late_events \
        .process(LateEventProcessFunctionImpl()) \
        .name("late-event-metrics")
    

    # 设置输出目录路径
    if output_dir:
        a = output_dir.rstrip('/')
        output_dir = f"{a}/window" # 窗口结果目录
        late_output_dir = f"{a}/late" # 迟到数据目录

    
    if output_dir:
        # 创建 FileSink
        sink = FileSink \
            .for_row_format(output_dir, Encoder.simple_string_encoder("UTF-8")) \
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("window-result") # 文件前缀
                .with_part_suffix(".csv")          # 文件后缀
                .build()) \
            .with_rolling_policy(RollingPolicy.default_rolling_policy()) \
            .build()
        
        window_results.map(lambda x: str(x), output_type=Types.STRING()) \
            .sink_to(sink).name("window-results-file")
    else:
        window_results.print().name("window-results-print")

    # 配置迟到数据的 Sink
    if late_output_dir:
        late_sink = FileSink \
            .for_row_format(late_output_dir, Encoder.simple_string_encoder("UTF-8")) \
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("late-metric") # 文件前缀
                .with_part_suffix(".csv")        # 文件后缀
                .build()) \
            .with_rolling_policy(RollingPolicy.default_rolling_policy()) \
            .build()
            
        # 将迟到指标转换为字符串并写入文件
        late_metrics.map(lambda x: str(x), output_type=Types.STRING()) \
            .sink_to(late_sink).name("late-metrics-file")

    env.execute("PyFlink Watermark Latency Analysis")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Flink Watermark Latency Analysis in Python")
    parser.add_argument('--watermarkDelayMs', dest='watermark_delay_ms', type=int, default=5000)
    parser.add_argument('--windowSizeMs', dest='window_size_ms', type=int, default=10000)
    parser.add_argument('--windowSlideMs', dest='window_slide_ms', type=int, default=10000)
    parser.add_argument('--allowedLatenessMs', dest='allowed_lateness_ms', type=int, default=0)
    parser.add_argument('--windowType', dest='window_type', type=str, default='tumbling')
    parser.add_argument('--eventsPerSecond', dest='events_per_second', type=int, default=500)
    parser.add_argument('--runDurationMs', dest='run_duration_ms', type=int, default=60000)
    parser.add_argument('--maxOutOfOrdernessMs', dest='max_out_of_orderness_ms', type=int, default=5000)
    parser.add_argument('--lateEventFraction', dest='late_event_fraction', type=float, default=0.2)
    parser.add_argument('--severeLatenessUpperBoundMs', dest='severe_lateness_upper_bound_ms', type=int, default=5000)
    parser.add_argument('--parallelism', dest='parallelism', type=int, default=6)
    parser.add_argument('--autoWatermarkIntervalMs', dest='auto_watermark_interval_ms', type=int, default=200)
    parser.add_argument('--enableCheckpointing', dest='enable_checkpointing', type=bool, default=False)
    parser.add_argument('--checkpointIntervalMs', dest='checkpoint_interval_ms', type=int, default=60000)
    parser.add_argument('--outputDir', dest='output_dir', type=str, default=None)

    args = parser.parse_args()
    if args.max_out_of_orderness_ms == -1: 
        args.max_out_of_orderness_ms = args.watermark_delay_ms

    # 运行作业
    run_job(args)