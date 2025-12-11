## 研究目的
探究Glink水位线延迟时间对窗口出发与结果准确性的影响

## 研究内容
深入理解Flink的水位线（Watermark）机制，探究水位线延迟时间对窗口触发延迟和结果计算准确度的影响。进一步研究在乱序数据场景下，如何通过合理设置水位线延迟间，在延迟与计算准确性之间取得平衡。

## 实验

### 实验环境
* 硬件：
- 3节点阿里云服务器
- 4vCPU
- 8G内存
- 内网带宽：2000Mbps
- 外网带宽: 10Mbps
- 存储: 40G SSD

* 软件：
- 操作系统: Ubuntu 24.04 
- JDK 版本: openjdk 11
- flink 版本: 1.17.2
- python版本：3.10
- minio 版本：RELEASE.2025-09-07T16-13-09Z 

### 实验负载
#### 负载信息
本实验基于Flink 1.17.2版本构建，使用3节点阿里云服务器组成集群，WebUI中设置的可用内存等负载配置如下：

**Job Manager内存配置：**
- JVM Heap: 5.02 GB
- Off-Heap Memory: 128 MB
- JVM Metaspace: 256 MB
- JVM Overhead: 614 MB

**Task Manager内存配置（每节点）：**
- JVM Heap Size: 512 MB
- Task Heap: 384 MB
- Managed Memory: 512 MB
- Framework Off-Heap: 128 MB
- Network: 128 MB
- JVM Metaspace: 256 MB

**集群资源：**
- 每节点Slots数: 2
- 集群总Slots数: 6

#### 数据生成
利用 Flink 内置的`datagen`连接器，使用`events_per_second`参数,控制数据生成速度并生成模拟的`UserEvent`事件流
##### 1. 概述

通过 **Flink DataGen 连接器** 配合自定义的 **Python MapFunction** 在内存中实时生成数据。

数据生成的核心逻辑封装在 `exp.py` 的 `EventGeneratorMapFunction` 类中，模拟具有随机延迟和迟到特性的用户行为数据流。

##### 2. 数据构建核心流程

数据生成主要分为两个步骤：**序列生成** 和 **业务模拟**。

##### 第一步：基础序列流 (Source)

利用 Flink SQL 的 `datagen` 连接器生成基础触发流。

- **生成内容**：简单的递增数字序列 (Sequence)。

- **生成速率**：由 `events_per_second` 参数严格控制。

- **生成规模**：通过 `run_duration_ms` 参数控制总生成量。
##### 第二步：业务属性模拟 (Map Transformation)

将上述序列通过 `EventGeneratorMapFunction` 转换为具有业务含义的 `UserEvent` 对象。每个事件包含以下核心字段的模拟逻辑：

1.  **基准时间 (`base_timestamp`)**：

    * 当前的系统物理时间。

2.  **事件时间 (`event_time`)**：

    - **正常乱序**：在基准时间基础上，减去一个 `0` 到 `max_out_of_orderness_ms` 之间的随机延迟。

    - **迟到数据 (Late Event)**：如果命中迟到概率，会额外再减去一段时长，强制生成早于水位线的时间戳，用于测试 Flink 的迟到丢弃机制。

3.  **用户 ID (`user_id`)**：

    * 在 `1` 到 `100` 之间随机生成整数 (例如: `user-42`)。

4.  **事件类型 (`event_type`)**：

    * 从 `["click", "view", "purchase", "logout"]` 中随机选择。

5.  **金额 (`amount`)**：

    * 生成 `1.0` 到 `500.0` 之间的随机浮点数。
---

##### 3. 数据集控制参数

通过命令行参数可以精准控制生成数据的**速度**、**规模**以及**乱序程度**。

##### A. 流量与规模控制

| 参数名称 (命令行) | 代码变量名 | 作用解释 | 默认值 |
| :--- | :--- | :--- | :--- |
| `--eventsPerSecond` | `events_per_second` | **生成速率**。<br>控制每秒产生多少条数据。 | 500 |
| `--runDurationMs` | `run_duration_ms` | **数据总量控制**。<br>设定程序运行多久后停止生成数据。 | 60000 |


##### B. 时间乱序控制

这部分参数决定了数据流中时间的“混乱程度”，直接决定了实验中窗口触发的时机和迟到数据的比例。

##### 1. `--maxOutOfOrdernessMs`

* **变量名**：`max_out_of_orderness_ms`

* **含义**：正常的数据乱序范围。

* **解释**：模拟正常的网络延迟。例如设为 5000ms，表示生成的事件时间可能比当前物理时间晚 0~5 秒。这是 Watermark 通常能够容忍的范围。

* **默认值**：5000

##### 2. `--lateEventFraction`

* **变量名**：`late_event_fraction`

* **含义**：迟到数据的比例。

* **解释**：决定了数据集中有多少数据是“严重迟到”的（即人为制造的脏数据）。例如设为 0.5，表示 50% 的数据会被故意伪造为过期数据。

* **默认值**：0.2

##### 3. `--severeLatenessUpperBoundMs`

* **变量名**：`severe_lateness_upper_bound_ms`

* **含义**：严重迟到的追加时长。

* **解释**：对于被判定为“严重迟到”的数据，让它们在“最大乱序时间”的基础上再“旧”多久。

* **计算公式**：

    
    迟到事件时间 = 当前时间 - (正常乱序最大值 + 随机严重迟到值)
    

* **默认值**：5000

---


### 实验步骤
#### 1. 集群部署与配置
1. **环境准备**
   - 在3个阿里云服务器节点上安装Ubuntu 24.04操作系统
   - 安装OpenJDK 11和Python 3.10
   - 配置节点间免密SSH登录
   - 安装uv
   - 安装MinIO作为对象存储（用来汇总实验结果）

2. **Flink集群部署**
   - 下载并解压Flink 1.17.2安装包
   - 配置JobManager（主节点）：
     - RPC地址：172.16.95.220，端口：6123
     - 绑定地址：0.0.0.0
     - 进程内存：6144m
   - 配置3个TaskManager（工作节点）：
     - 绑定地址：0.0.0.0
     - 节点地址：172.16.95.220/221/222
     - 进程内存：1728m
     - 每节点Task Slots：2
   - 配置s3文件系统连接MinIO
    ```bash
        cd /root/flink-1.17.2
        mkdir -p plugins/s3-fs-hadoop
        cp opt/flink-s3-fs-hadoop-1.17.2.jar plugins/s3-fs-hadoop/
    ```
    修改`conf/flink-conf.yaml`:
    ```ymal
    s3.endpoint: http://localhost:9000
    s3.path.style.access: true
    s3.access-key: minioadmin                
    s3.secret-key: minioadmin        
    s3.ssl.enabled: false
    ``` 
   - 启动Flink集群：`./bin/start-cluster.sh`

3. **集群状态验证**
   - 通过Flink Web UI（默认端口8081）验证集群状态
   - 检查TaskManager节点是否正常注册
   - 确认集群资源配置符合预期

   ![Flink JobManager 配置页](asset/img/Flink%20JobManager%20配置页.png)
   
   上图展示了Flink JobManager的配置信息，包括JVM内存配置、端口设置等关键参数，确保集群配置正确。

   ![Flink TaskManager 集群概览页](asset/img/Flink%20TaskManager%20集群概览页.png)
   
   上图显示了Flink集群中3个TaskManager节点的概览信息，包括节点地址、资源配置和运行状态，确认所有节点均已成功注册。
   
   ![TaskManager (220 节点) 内存指标页](asset/img/TaskManager%20(220%20节点)%20内存指标页.jpg)
   
   上图展示了220节点的TaskManager内存使用情况，包括JVM堆内存、非堆内存和直接内存的使用情况，确保节点资源使用正常。
   
   ![TaskManager (221 节点) 内存指标页](asset/img/TaskManager%20(221%20节点)%20内存指标页.png)
   
   上图展示了221节点的TaskManager内存使用情况，包括JVM堆内存、非堆内存和直接内存的使用情况，确保节点资源使用正常。
   
   ![TaskManager (222 节点) 内存指标页](asset/img/TaskManager%20(222%20节点)%20内存指标页.png)
   
   上图展示了222节点的TaskManager内存使用情况，包括JVM堆内存、非堆内存和直接内存的使用情况，确保节点资源使用正常。


#### 2. 实验代码准备与运行
1. **代码获取**
   - 从代码仓库克隆实验代码到每个节点
   - 进入代码目录：`cd G_Flink_Watermark/code`
   - 安装依赖 `uv sync`
2. **启动集群与minio**
    - 启动minio服务并创建bucket:`flink-bucket`
    - 启动flink集群 `./bin/start-cluster.sh`
2. **作业提交**
    ```bash
    flink run \
    -pyexec {workdir}/.venv/bin/python \
    -pyclientexec {workdir}/.venv/bin/python \
    -py {workdir}/exp.py \
    --outputDir s3://flink-bucket/{experiment_results/}  # 输出结果目录
    ```


#### 3. 实验执行
1. **实验场景设计**
   本次实验共设计了9个不同的实验场景，涵盖水位线延迟、允许迟到时间、乱序程度、窗口类型和并行度等关键参数的影响分析：

   **场景1：不同水位线延迟配置**
   - 低延迟配置（激进策略）：水位线延迟1秒，预期产生大量迟到数据
   - 中等延迟配置（平衡策略）：水位线延迟5秒，与最大乱序度持平
   - 高延迟配置（保守策略）：水位线延迟10秒，预期迟到数据极少

   **场景2：不同允许迟到时间配置**
   - 允许迟到2秒：尝试救回部分迟到数据
   - 允许迟到10秒：尝试救回绝大部分迟到数据

   **场景3：极度乱序输入测试**
   - 乱序程度8秒，远超水位线延迟5秒，测试系统鲁棒性

   **场景4：滑动窗口配置**
   - 窗口大小10秒，滑动步长5秒，测试滑动窗口的水位线处理特性

   **场景5：不同并行度配置**
   - 高并行度（6）：测试高并发下的水位线机制表现
   - 低并行度（2）：测试低并发下的水位线机制表现

2. **参数配置要点**
   所有实验共享以下基础配置：
   - 事件生成速率：500-1000个/秒
   - 实验运行时长：60秒
   - 窗口大小：10秒
   - 最大乱序度：5000毫秒
   - 严重迟到事件比例：0.1-0.3
   - 严重迟到事件上界：5000毫秒
   - 自动水位线间隔：200毫秒
   - 检查点：禁用

   每个场景通过调整以下核心参数实现差异化测试：
   - 水位线延迟（watermarkDelayMs）：1000ms, 5000ms, 10000ms
   - 允许迟到时间（allowedLatenessMs）：0ms, 2000ms, 10000ms
   - 窗口类型（windowType）：tumbling, sliding
   - 窗口滑动步长（windowSlideMs）：5000ms, 10000ms
   - 并行度（parallelism）：2, 4, 6

3. **作业提交与监控**
   - 使用Flink CLI提交作业，通过参数组合实现不同实验场景
   - 在Web UI中实时监控作业运行状态和资源使用情况
   - 观察TaskManager节点的CPU、内存和网络使用情况
   - 记录作业执行日志，关注异常信息和性能指标

   ![Flink JobManager 内存指标页](asset/img/Flink%20JobManager%20内存指标页.png)
   
   上图展示了JobManager的内存使用情况，确保在作业执行过程中资源使用正常，没有出现内存溢出等问题。

   ![Flink 已完成任务列表页](asset/img/Flink%20已完成任务列表页.png)
   
   上图显示了Flink已完成的任务列表，包含了不同参数配置下的水位线分析作业，验证了实验的可重复性和完整性。

#### 4. 数据收集与分析
1. **结果数据收集**
   - 从指定输出目录收集窗口聚合结果
   - 从侧输出目录收集迟到事件指标

2. **数据预处理**
   - 清理和转换原始数据
   - 格式化数据以便于分析和可视化

3. **数据分析**
   - 基于收集的数据进行统计分析
   - 生成各类图表，如散点图、折线图、直方图等

#### 5. 实验验证与调优
1. **结果验证**
   - 验证实验结果的正确性和一致性
   - 检查是否存在异常数据或错误

2. **性能调优**
   - 根据实验结果调整集群配置
   - 优化作业参数，如并行度、窗口大小等

3. **重复实验**
   - 在调整配置后重新执行实验
   - 比较调优前后的实验结果

   ![Flink 已完成任务列表页](asset/img/Flink%20已完成任务列表页.png)
   
   上图显示了Flink已完成的任务列表，包含了不同参数配置下的水位线分析作业，验证了实验的可重复性和完整性。

### 实验结果与分析
使用表格和图表直观呈现结果，并解释结果背后的原因。

### 结论
总结研究的主要发现。

### 分工
尽可能详细地写出每个人的具体工作和贡献度，并按贡献度大小进行排序。
