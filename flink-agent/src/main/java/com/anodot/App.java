package com.anodot;

import net.galgus.flink.streaming.connectors.http.common.HTTPConnectionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import net.galgus.flink.streaming.connectors.http.HTTPSink;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Hashtable;
import java.util.Map;

public class App {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
                BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        JSONObject pipeline_config = (new ConfigProvider()).get_config("test_dir_csv");
        JSONObject source = (JSONObject) pipeline_config.get("source");
        JSONObject destination = (JSONObject) pipeline_config.get("destination");

        for (int i = 0; i < 3; i ++) {
//            env.addSource()
            JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                    .setDrivername("com.mysql.cj.jdbc.Driver")
//                    .setDBUrl("jdbc:mysql://root@127.0.0.1:3308/test")
                    .setDBUrl((String) source.get("url"))
                    .setQuery("SELECT clicks, impressions FROM test")
                    .setRowTypeInfo(rowTypeInfo)
                    .finish();

            DataStream<JSONArray> data = env.createInput(jdbcInputFormat)
                    .flatMap(new Transformer());

            data.addSink(new HTTPSink<JSONArray>(
                    new HTTPConnectionConfig(
//                        "http://localhost:8080/api/v1/metrics?token=asdf&protocol=anodot20",
                            destination.get("url") + "/api/v1/metrics?token=token&protocol=anodot20",
                        HTTPConnectionConfig.HTTPMethod.POST,
                        getHeaders(),
                        false
                    )
            ));
            env.execute();
        }
    }

    private static Map<String, String> getHeaders() {
        Map<String, String> headers = new Hashtable<>();
        headers.put("Content-Type", "application/json");
        return headers;
    }

    public static class Transformer implements FlatMapFunction<Row, JSONArray> {
        @Override
        public void flatMap(Row row, Collector<JSONArray> out) {
            JSONObject metric = new JSONObject();
            metric.put("timestamp", 1512867600);
            metric.put("properties", dimensions());
            metric.put("value", row.getField(0));
            metric.put("tags", tags());
            JSONArray metrics = new JSONArray();
            out.collect(metrics.put(metric));
        }

        private JSONObject dimensions() {
            JSONObject dimensions = new JSONObject();
            dimensions.put("adsize", "Sma_ll");
            dimensions.put("country", "USA");
            dimensions.put("what", "clicks");
            return dimensions;
        }

        private JSONObject tags() {
            return new JSONObject("{\"source\": [\"anodot-agent\"],\"source_host_id\": [\"host_id\"],\"source_host_name\": [\"agent\"],\"pipeline_id\": [\"test_flink\"],\"pipeline_type\": [\"mysql\"] }");
        }
    }

    public static class ConfigProvider {
        public JSONObject get_config(String pipeline_id) throws IOException {
            URL url = new URL("http://localhost/pipelines/" + pipeline_id);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("Content-Type", "application/json");
            con.setConnectTimeout(5000);
            con.setReadTimeout(5000);

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            con.disconnect();

            return new JSONObject(content.toString());
        }
    }
}
