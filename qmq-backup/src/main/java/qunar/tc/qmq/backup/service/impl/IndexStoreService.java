package qunar.tc.qmq.backup.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.store.impl.IndexDbDao;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.function.Consumer;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

/**
 * @Classname IndexStoreService
 * @Description 保存index至数据库中
 * @Date 11.8.21 10:59 上午
 * @Created by zhipeng.cai
 */
public class IndexStoreService {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexStoreService.class);

    private final DynamicConfig skipBackSubjects;
    private final String brokerGroup;
    private final IndexDbDao indexDbDao;
    private MessageQueryIndex lastIndex;

    public IndexStoreService() {
        this.skipBackSubjects = DynamicConfigLoader.load("skip_backup.properties", false);
        this.brokerGroup = BrokerConfig.getBrokerName();
        this.indexDbDao = new IndexDbDao();
    }

    public void appendData(MessageQueryIndex index, Consumer<MessageQueryIndex> consumer) {
        lastIndex = index;
        String subject = index.getSubject();
        String realSubject = RetrySubjectUtils.getRealSubject(subject);
        if (skipBackup(realSubject)) {
            return;
        }
        monitorBackupIndexQps(subject);
        String subjectKey = realSubject;
        String consumerGroup = null;
        if (RetrySubjectUtils.isRetrySubject(subject)) {
            subjectKey = RetrySubjectUtils.buildRetrySubject(realSubject);
            consumerGroup = RetrySubjectUtils.getConsumerGroup(subject);
        }
        try {
            indexDbDao.insertIndex(subjectKey, index.getMessageId(), brokerGroup, consumerGroup, index.getCreateTime(), index.getSequence());
        } catch (Exception e) {
            LOGGER.error("消息索引插入数据库失败", e);
        }
        if (consumer != null) consumer.accept(lastIndex);
    }

    private boolean skipBackup(String subject) {
        return skipBackSubjects.getBoolean(subject, false);
    }

    private static void monitorBackupIndexQps(String subject) {
        Metrics.meter("backup.message.index.qps", SUBJECT_ARRAY, new String[]{subject}).mark();
    }
}
