package qunar.tc.qmq.backup.store.impl;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @Classname IndexDbDao
 * @Description 保存index至mysql
 * @Date 11.8.21 10:47 上午
 * @Created by zhipeng.cai
 */
public class IndexDbDao {
    private final JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();
    private final String insertSql;

    public IndexDbDao() {
        String table = "qmq_backup";
        this.insertSql = String.format("INSERT INTO %s(subjectId,messageId,brokerGroup,consumerGroup,time,sequence) VALUES(?,?,?,?,?,?)", table);
    }

    public int insertIndex(String subjectId, String messageId, String brokerGroup, String consumerGroup, long time, long sequence) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        PreparedStatementCreator psc = connection -> {
            PreparedStatement ps = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, subjectId);
            ps.setString(2, messageId);
            ps.setString(3, brokerGroup);
            ps.setString(4, consumerGroup);
            ps.setDate(5, new Date(time));
            ps.setLong(6, sequence);
            return ps;
        };
        jdbcTemplate.update(psc, keyHolder);
        return keyHolder.getKey().intValue();
    }

    public int[] insertBatchIndex(List<Object[]> indices) {
        return jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Object[] params = indices.get(i);
                ps.setObject(1, params[0]);
                ps.setObject(2, params[1]);
                ps.setObject(3, params[2]);
                ps.setObject(4, params[3]);
                ps.setObject(5, new Date((long) params[4]));
                ps.setObject(6, params[5]);
            }

            @Override
            public int getBatchSize() {
                return indices.size();
            }
        });
    }
}
