package com.amazonaws.services.msf;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * A basic Flink Java application to run on Amazon Managed Service for Apache Flink,
 * with Kinesis Data Streams as source and sink.
 */
public class BasicStreamingJob {

    private static final Logger LOGGER = LogManager.getLogger(BasicStreamingJob.class);

    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "application-properties.json";

    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        Map<String, Properties> applicationParameters;

        if (env instanceof LocalStreamEnvironment) {
            applicationParameters = KinesisAnalyticsRuntime.getApplicationProperties(
                    BasicStreamingJob.class.getClassLoader()
                            .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath());
        } else {
            applicationParameters = KinesisAnalyticsRuntime.getApplicationProperties();
        }

        if (applicationParameters == null || applicationParameters.isEmpty()) {
            throw new IllegalArgumentException("Application properties not loaded or empty.");
        }
        return applicationParameters;
    }

    private static FlinkKinesisConsumer<String> createSource(Properties inputProperties) {
        if (inputProperties == null || inputProperties.getProperty("stream.name") == null) {
            throw new IllegalArgumentException("Input properties or stream name is missing.");
        }
        String inputStreamName = inputProperties.getProperty("stream.name");
        return new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties);
    }

    private static KinesisStreamsSink<String> createSink(Properties outputProperties) {
        if (outputProperties == null || outputProperties.getProperty("stream.name") == null) {
            throw new IllegalArgumentException("Output properties or stream name is missing.");
        }
        String outputStreamName = outputProperties.getProperty("stream.name");
        return KinesisStreamsSink.<String>builder()
                .setKinesisClientProperties(outputProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setStreamName(outputStreamName)
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .build();
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final Map<String, Properties> applicationParameters = loadApplicationProperties(env);

        Properties inputProperties = applicationParameters.get("InputStream0");
        Properties outputProperties = applicationParameters.get("OutputStream0");

        if (inputProperties == null || inputProperties.isEmpty()) {
            throw new IllegalArgumentException("InputStream0 properties are required.");
        }

        if (outputProperties == null || outputProperties.isEmpty()) {
            throw new IllegalArgumentException("OutputStream0 properties are required.");
        }

        SourceFunction<String> source = createSource(inputProperties);
        DataStream<String> input = env.addSource(source, "Kinesis Source");

        Sink<String> sink = createSink(outputProperties);
        input.sinkTo(sink);

        env.execute("Flink streaming Java API skeleton");
    }
}
