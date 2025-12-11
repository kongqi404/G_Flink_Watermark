# /root/flink_exp/model.py
# 导入 Python 标准库 time 模块，用于处理时间
import time
# 导入 uuid 模块，用于生成唯一标识符（本代码中暂未使用，但常用于生成 ID）
import uuid
# 导入 random 模块，用于生成随机数
import random
# 从 PyFlink 导入 TimestampAssigner 接口，用于自定义时间戳提取逻辑
from pyflink.common.watermark_strategy import TimestampAssigner

# ==========================================
# 数据类 (Data Classes)
# ==========================================

# 定义 UserEvent 类，代表输入的数据流中的单个事件
class UserEvent:
    # 初始化方法，定义事件的属性
    def __init__(self, user_id=None, event_type=None, event_time=0, amount=0.0, ingestion_time=0):
        self.user_id = user_id              # 用户 ID，作为分组的 Key
        self.event_type = event_type        # 事件类型 (如 click, view)
        self.event_time = event_time        # 事件发生的时间戳 (Event Time)
        self.amount = amount                # 事件涉及的金额/数值
        self.ingestion_time = ingestion_time # 数据进入系统的时间 (Processing Time)，用于对比

    # 定义对象的字符串表示形式，方便打印调试
    def __repr__(self):
        return f"UserEvent(user_id='{self.user_id}', event_type='{self.event_type}', event_time={self.event_time}, amount={self.amount}, ingestion_time={self.ingestion_time})"

# 定义 EventAccumulator 类，作为聚合函数的累加器 (中间状态)
class EventAccumulator:
    # 初始化累加器状态
    def __init__(self):
        self.count = 0                      # 记录事件总数
        self.sum_amount = 0.0               # 记录金额总和
        self.min_event_time = float('inf')  # 记录窗口内最早的事件时间 (初始化为无穷大)
        self.max_event_time = float('-inf') # 记录窗口内最晚的事件时间 (初始化为无穷小)
        self.min_amount = float('inf')      # 记录最小金额
        self.max_amount = float('-inf')     # 记录最大金额

    # 定义 add 方法：将单个 UserEvent 数据添加到累加器中
    def add(self, event: UserEvent):
        self.count += 1                                     # 计数 +1
        self.sum_amount += event.amount                     # 累加金额
        self.min_event_time = min(self.min_event_time, event.event_time) # 更新最小时间
        self.max_event_time = max(self.max_event_time, event.event_time) # 更新最大时间
        self.min_amount = min(self.min_amount, event.amount)             # 更新最小金额
        self.max_amount = max(self.max_amount, event.amount)             # 更新最大金额

    # 定义 merge 方法：合并两个累加器 (用于 Session Window 或两阶段聚合)
    def merge(self, other):
        self.count += other.count                           # 合并计数
        self.sum_amount += other.sum_amount                 # 合并总和
        self.min_event_time = min(self.min_event_time, other.min_event_time) # 取两者中更早的时间
        self.max_event_time = max(self.max_event_time, other.max_event_time) # 取两者中更晚的时间
        self.min_amount = min(self.min_amount, other.min_amount)             # 取两者中更小的金额
        self.max_amount = max(self.max_amount, other.max_amount)             # 取两者中更大的金额

    # 辅助方法：计算平均值
    def get_average_amount(self):
        # 防止除以零错误
        return 0.0 if self.count == 0 else self.sum_amount / self.count

# 定义 WindowAggregateResult 类，代表窗口计算的最终输出结果
class WindowAggregateResult:
    # 初始化结果对象的各个字段
    def __init__(self, user_id, window_start, window_end, configured_watermark_delay_ms,
                 current_watermark, trigger_lag_ms, trigger_system_time,
                 event_count, sum_amount, average_amount, min_amount, max_amount,
                 min_event_time, max_event_time):
        self.user_id = user_id                              # 窗口所属的用户 ID
        self.window_start = window_start                    # 窗口开始时间
        self.window_end = window_end                        # 窗口结束时间
        self.configured_watermark_delay_ms = configured_watermark_delay_ms # 配置的水位线延迟
        self.current_watermark = current_watermark          # 触发时的当前水位线
        self.trigger_lag_ms = trigger_lag_ms                # 触发滞后时间 (水位线 - 窗口结束时间)
        self.trigger_system_time = trigger_system_time      # 触发时的系统物理时间
        self.event_count = event_count                      # 统计：数量
        self.sum_amount = sum_amount                        # 统计：总和
        self.average_amount = average_amount                # 统计：平均值
        self.min_amount = min_amount                        # 统计：最小金额
        self.max_amount = max_amount                        # 统计：最大金额
        self.min_event_time = min_event_time                # 统计：最早事件时间
        self.max_event_time = max_event_time                # 统计：最晚事件时间

    # 定义字符串表示，用于输出结果
    def __str__(self):
        return (f"WindowAggregateResult{{user_id='{self.user_id}', "
                f"window_start={self.window_start}, window_end={self.window_end}, "
                f"delay={self.configured_watermark_delay_ms}, watermark={self.current_watermark}, "
                f"lag={self.trigger_lag_ms}, count={self.event_count}}}")

# 定义 LateEventMetric 类，用于封装迟到数据的信息
class LateEventMetric:
    # 初始化迟到指标
    def __init__(self, user_id, event_time, detection_time, lateness_ms):
        self.user_id = user_id              # 迟到数据的用户ID
        self.event_time = event_time        # 数据的事件时间
        self.detection_time = detection_time # 侧输出流处理该数据的时间
        self.lateness_ms = lateness_ms      # 迟到了多久 (当前水位线 - 事件时间)

    # 字符串表示
    def __str__(self):
        return f"LateEventMetric{{user_id='{self.user_id}', event_time={self.event_time}, detection_time={self.detection_time}, lateness_ms={self.lateness_ms}}}"

# 定义自定义的时间戳提取器，告诉 Flink 哪个字段是 Event Time
class CustomTimestampAssigner(TimestampAssigner):
    # 实现 extract_timestamp 方法
    def extract_timestamp(self, value, record_timestamp):
        # 返回 UserEvent 对象中的 event_time 字段作为 Flink 的内部时间戳
        return int(value.event_time)