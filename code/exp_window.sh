flink run \
    -pyexec /root/flink_exp/.venv/bin/python \
    -pyclientexec /root/flink_exp/.venv/bin/python \
    -py /root/flink_exp/exp.py \
    --watermarkDelayMs 3000 \
    --windowSizeMs 2000 \
    --windowSlideMs 10000 \
    --allowedLatenessMs 0 \
    --windowType tumbling \
    --eventsPerSecond 500 \
    --runDurationMs 60000 \
    --maxOutOfOrdernessMs 5000 \
    --lateEventFraction 0.2 \
    --severeLatenessUpperBoundMs 5000 \
    --parallelism 4 \
    --autoWatermarkIntervalMs 200 \
    --enableCheckpointing false \
    --checkpointIntervalMs 10000 \
    --outputDir s3://flink-bucket/ws_2000/


flink run \
    -pyexec /root/flink_exp/.venv/bin/python \
    -pyclientexec /root/flink_exp/.venv/bin/python \
    -py /root/flink_exp/exp.py \
    --watermarkDelayMs 3000 \
    --windowSizeMs 6000 \
    --windowSlideMs 10000 \
    --allowedLatenessMs 0 \
    --windowType tumbling \
    --eventsPerSecond 500 \
    --runDurationMs 60000 \
    --maxOutOfOrdernessMs 5000 \
    --lateEventFraction 0.2 \
    --severeLatenessUpperBoundMs 5000 \
    --parallelism 4 \
    --autoWatermarkIntervalMs 200 \
    --enableCheckpointing false \
    --checkpointIntervalMs 10000 \
    --outputDir s3://flink-bucket/ws_6000/



flink run \
    -pyexec /root/flink_exp/.venv/bin/python \
    -pyclientexec /root/flink_exp/.venv/bin/python \
    -py /root/flink_exp/exp.py \
    --watermarkDelayMs 3000 \
    --windowSizeMs 10000 \
    --windowSlideMs 10000 \
    --allowedLatenessMs 0 \
    --windowType tumbling \
    --eventsPerSecond 500 \
    --runDurationMs 60000 \
    --maxOutOfOrdernessMs 5000 \
    --lateEventFraction 0.2 \
    --severeLatenessUpperBoundMs 5000 \
    --parallelism 4 \
    --autoWatermarkIntervalMs 200 \
    --enableCheckpointing false \
    --checkpointIntervalMs 10000 \
    --outputDir s3://flink-bucket/ws_10000/

flink run \
    -pyexec /root/flink_exp/.venv/bin/python \
    -pyclientexec /root/flink_exp/.venv/bin/python \
    -py /root/flink_exp/exp.py \
    --watermarkDelayMs 3000 \
    --windowSizeMs 12000 \
    --windowSlideMs 10000 \
    --allowedLatenessMs 0 \
    --windowType tumbling \
    --eventsPerSecond 500 \
    --runDurationMs 60000 \
    --maxOutOfOrdernessMs 5000 \
    --lateEventFraction 0.2 \
    --severeLatenessUpperBoundMs 5000 \
    --parallelism 4 \
    --autoWatermarkIntervalMs 200 \
    --enableCheckpointing false \
    --checkpointIntervalMs 10000 \
    --outputDir s3://flink-bucket/ws_12000/