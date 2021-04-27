package recordprocessor;


import com.alibaba.dts.formats.avro.Record;
import common.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordgenerator.OffsetCommitCallBack;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static common.Util.require;
import static common.Util.sleepMS;


/**
 * 此演示展示如何从字节中解析Avro记录
 * 我们将展示如何从反序列化记录中打印列
 */
public class EtlRecordProcessor implements  Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(EtlRecordProcessor.class);
    // 偏移回调
    private final OffsetCommitCallBack offsetCommitCallBack;
    // 提交检查点
    private volatile Checkpoint commitCheckpoint;
    // 提交线程
    private WorkThread commitThread;

    // 向阻塞队列添加kafka消费记录
    public boolean offer(long timeOut, TimeUnit timeUnit, ConsumerRecord record) {
        try {
            return toProcessRecord.offer(record, timeOut, timeUnit);
        } catch (Exception e) {
            log.error("EtlRecordProcessor: offer record failed, record[" + record + "], cause " + e.getMessage(), e);
            return false;
        }
    }

    // kafka消费阻塞队列
    private final LinkedBlockingQueue<ConsumerRecord> toProcessRecord;
    // 序列化器
    private final AvroDeserializer fastDeserializer;
    // 上下文
    private final Context context;
    // 记录监听器
    private final Map<String, RecordListener> recordListeners = new HashMap<>();

    // 是否存在消费线程
    private volatile boolean existed = false;
    public EtlRecordProcessor(OffsetCommitCallBack offsetCommitCallBack, Context context) {
        this.offsetCommitCallBack = offsetCommitCallBack;
        this.toProcessRecord = new LinkedBlockingQueue<>(512);
        fastDeserializer = new AvroDeserializer();
        this.context = context;
        commitCheckpoint = new Checkpoint(null, -1, -1, "-1");
        commitThread = getCommitThread();
        commitThread.start();
    }


    @Override
    public void run() {
        while (!existed) {
            ConsumerRecord<byte[], byte[]> toProcess = null;
            Record record = null;
            int fetchFailedCount = 0;
            try {
                // 获得记录
                while (null == (toProcess = toProcessRecord.peek()) && !existed) {
                    sleepMS(5);
                    fetchFailedCount++;
                    if (fetchFailedCount % 1000 == 0) {
                        log.info("EtlRecordProcessor: haven't receive records from generator for  5s");
                    }
                }
                if (existed) {
                    return;
                }
                fetchFailedCount = 0;
                final ConsumerRecord<byte[], byte[]> consumerRecord = toProcess;
                // 48 means an no op bytes, we use this bytes to push up offset. user should ignore this record
                if (consumerRecord.value().length == 48) {
                    continue;
                }
                // 反序列化记录
                record = fastDeserializer.deserialize(consumerRecord.value());
                log.debug("EtlRecordProcessor: meet [{}] record type", record.getOperation());
                // 向所有的监听者发送记录信息
                for (RecordListener recordListener : recordListeners.values()) {
                    recordListener.consume(new UserRecord(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset(), record, new UserCommitCallBack() {
                        @Override
                        public void commit(TopicPartition tp, Record commitRecord, long offset, String metadata) {
                            commitCheckpoint = new Checkpoint(tp, commitRecord.getSourceTimestamp(), offset, metadata);
                        }
                    }));
                }
                // 删除已经处理的记录
                toProcessRecord.poll();
            } catch (Exception e) {
                log.error("EtlRecordProcessor: process record failed, raw consumer record [" + toProcess + "], parsed record [" + record + "], cause " + e.getMessage(), e);
                existed = true;
            }
        }
    }

    // user define how to commit
    private void commit() {
        if (null != offsetCommitCallBack) {
            if (commitCheckpoint.getTopicPartition() != null && commitCheckpoint.getOffset() != -1) {
                log.info("commit record with checkpoint {}", commitCheckpoint);
                offsetCommitCallBack.commit(commitCheckpoint.getTopicPartition(), commitCheckpoint.getTimeStamp(),
                        commitCheckpoint.getOffset(), commitCheckpoint.getInfo());
            }
        }
    }

    public void registerRecordListener(String name, RecordListener recordListener) {
        require(null != name && null != recordListener, "null value not accepted");
        recordListeners.put(name, recordListener);
    }

    public  void close() {
        this.existed = true;
        commitThread.stop();
    }

    private WorkThread getCommitThread() {
        WorkThread workThread = new WorkThread(new Runnable() {
            @Override
            public void run() {
                while (!existed) {
                    sleepMS(5000);
                    commit();
                }
            }
        });
        return workThread;
    }

}
