/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.toll.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

/**
 *
 * @author dataeng
 */
public class VehicleCountAggStreamApp {

    /**
     *
     */
    static final Serde<JsonNode> JSON_SERDE = Serdes.serdeFrom(new JsonSerializer(),
            new JsonDeserializer());

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("please enter the names of the source and targe topics as arguments");
            System.exit(-1);
        }
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "vehicle-count-per-toll-period");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

        //read the input from the source topic
        builder.stream(args[0], Consumed.with(Serdes.String(), JSON_SERDE)).
                map((String k, JsonNode v) -> {
                    JsonNode key = getKeyAsNode(v);
                    return KeyValue.pair(key, "");
                }).
                groupByKey(Grouped.with(JSON_SERDE, Serdes.String())).
                windowedBy(TimeWindows.of(Duration.ofMinutes(1))).
                count().
                toStream().
                map((Windowed<JsonNode> k, Long v) -> {
                    ObjectNode keyNode = ((ObjectNode) k.key());
                    ObjectNode winNode = JsonNodeFactory.instance.objectNode();

                    winNode.put("start", k.window().start());
                    winNode.put("end", k.window().end());

                    keyNode.set("period", winNode);
                    keyNode.put("count", v);

                    return KeyValue.pair("", (JsonNode) keyNode);
                }).
                to(args[1], Produced.with(Serdes.String(), JSON_SERDE));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
            e.printStackTrace(System.out);
        });

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));

        System.out.println(topology.describe());

        streams.start();

        // System.exit(0);
    }

    static JsonNode getKeyAsNode(JsonNode v) {
        JsonNode tollNode = v.get("toll");
        ObjectNode oNode = JsonNodeFactory.instance.objectNode();
        oNode.put("vehicle_class_code", v.get("vehicle_type").get("code").asText());
        oNode.put("toll_code", tollNode.get("code").asText());
        oNode.put("toll_city", tollNode.get("city").asText());
        oNode.put("toll_state", tollNode.get("state").asText());

        return oNode;
    }
}
