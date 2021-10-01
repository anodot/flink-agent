package com.anodot;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

public class PeriodicalJDBCSource implements SourceFunction<Row> {
    boolean stop = false;

    public void run(SourceContext<Row> sourceContext) throws Exception {
        while (!stop) {
            Thread.sleep(60);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
                    BasicTypeInfo.FLOAT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO
            };
            RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
            JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                    .setDrivername("com.mysql.cj.jdbc.Driver")
                    .setDBUrl("jdbc:mysql://root@127.0.0.1:3308/test")
//                    .setDBUrl((String) source.get("url"))
                    .setQuery("SELECT clicks, impressions FROM test")
                    .setRowTypeInfo(rowTypeInfo)
                    .finish();
            DataStreamSource<Row> s = env.createInput(jdbcInputFormat);
//            sourceContext.collect();
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
