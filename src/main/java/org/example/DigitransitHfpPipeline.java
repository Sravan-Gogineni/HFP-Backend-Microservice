package org.example;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DigitransitHfpPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(DigitransitHfpPipeline.class);

    public interface HfpOptions extends DataflowPipelineOptions {
        @Description("Pub/Sub topic ID (not full path, just the topic name)")
        @Validation.Required
        String getPubsubTopic();
        void setPubsubTopic(String value);

        @Description("Enable verbose message logging — use for local runs only")
        boolean getLogMessages();
        void setLogMessages(boolean value);
    }

    // ── Custom SDF MQTT Source ─────────────────────────────────────────────────
    static class MqttSdfFn extends DoFn<String, byte[]> {

        private final String brokerUrl;
        private final String topic;
        private final boolean logMessages;
        private transient MqttClient client;
        private transient LinkedBlockingQueue<byte[]> queue;

        MqttSdfFn(String brokerUrl, String topic, boolean logMessages) {
            this.brokerUrl = brokerUrl;
            this.topic = topic;
            this.logMessages = logMessages;
        }

        @Setup
        public void setup() throws Exception {
            queue = new LinkedBlockingQueue<>(10000);
            client = new MqttClient(brokerUrl, MqttClient.generateClientId());

            MqttConnectOptions opts = new MqttConnectOptions();
            opts.setCleanSession(true);
            opts.setAutomaticReconnect(true);
            opts.setConnectionTimeout(30);
            opts.setKeepAliveInterval(60);

            client.setCallback(new MqttCallback() {
                @Override
                public void messageArrived(String t, MqttMessage msg) {
                    try {
                        String[] topicLevels = t.split("/");
                        String vehicleType = topicLevels.length > 6 ? topicLevels[6] : "unknown";

                        JSONObject json = new JSONObject(new String(msg.getPayload(), StandardCharsets.UTF_8));
                        JSONObject vp = json.optJSONObject("VP");
                        if (vp != null) {
                            vp.put("VehicleType", vehicleType);
                        }
                        queue.offer(json.toString().getBytes(StandardCharsets.UTF_8));
                    } catch (Exception e) {
                        queue.offer(msg.getPayload());
                    }
                }
                @Override
                public void connectionLost(Throwable cause) {
                    LOG.warn("MQTT connection lost: {}", cause.getMessage());
                }
                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {}
            });

            client.connect(opts);
            client.subscribe(topic, 1);
            LOG.info("MQTT connected and subscribed to {}", topic);
        }

        @ProcessElement
        public void processElement(OutputReceiver<byte[]> out) throws Exception {
            while (true) {
                byte[] payload = queue.poll(1, TimeUnit.SECONDS);
                if (payload != null) {
                    if (logMessages) {
                        try {
                            JSONObject json = new JSONObject(new String(payload, StandardCharsets.UTF_8));
                            JSONObject vp = json.optJSONObject("VP");
                            if (vp != null) {
                                LOG.info("RouteID: {} VehicleType: {}",
                                        vp.optString("route", "N/A"),
                                        vp.optString("VehicleType", "N/A"));
                            }
                        } catch (Exception ignored) {}
                    }
                    out.output(payload);
                }
            }
        }

        @Teardown
        public void teardown() throws Exception {
            if (client != null && client.isConnected()) {
                client.disconnect();
                client.close();
                LOG.info("MQTT disconnected");
            }
        }
    }

    // ── Key Mapper ─────────────────────────────────────────────────────────────
    static class KeyMapper {
        static java.util.Map<String, String> getKeyMapping() {
            java.util.Map<String, String> mapping = new java.util.HashMap<>();
            mapping.put("desi", "RouteNumber");
            mapping.put("dir", "RouteDirection");
            mapping.put("oper", "OperatorID");
            mapping.put("veh", "VehicleNumber");
            mapping.put("tst", "Timestamp");
            mapping.put("tsi", "UnixTimestamp");
            mapping.put("spd", "Speed");
            mapping.put("hdg", "Heading");
            mapping.put("lat", "Latitude");
            mapping.put("long", "Longitude");
            mapping.put("acc", "Acceleration");
            mapping.put("dl", "ScheduleOffset");
            mapping.put("odo", "OdometerReading");
            mapping.put("drst", "DoorStatus");
            mapping.put("oday", "OperatingDay");
            mapping.put("jrn", "JourneyID");
            mapping.put("line", "LineID");
            mapping.put("start", "ScheduledDepartureTime");
            mapping.put("loc", "LocationSource");
            mapping.put("stop", "StopID");
            mapping.put("route", "RouteID");
            mapping.put("occu", "OccupancyLevel");
            return mapping;
        }

        static JSONObject renameKeys(JSONObject original) {
            JSONObject renamed = new JSONObject();
            java.util.Map<String, String> keyMap = getKeyMapping();
            original.keys().forEachRemaining(key -> {
                String newKey = keyMap.getOrDefault(key, key);
                renamed.put(newKey, original.get(key));
            });
            return renamed;
        }

        static Integer convertSpeedToKmh(Integer speedinms) {
            if (speedinms == null) return null;
            return (int) Math.round(speedinms * 3.6);
        }

        static Integer convertDistanceToKm(Integer distanceInMeters) {
            if (distanceInMeters == null) return null;
            return (int) Math.round(distanceInMeters / 1000.0);
        }
    }

    // ── Main ───────────────────────────────────────────────────────────────────
    public static void main(String[] args) {
        HfpOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(HfpOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        String projectId = options.getProject();
        String topicId = options.getPubsubTopic();
        boolean logMessages = options.getLogMessages();
        String brokerUrl = "ssl://mqtt.hsl.fi:8883";
        String topicPattern = "/hfp/v2/journey/ongoing/vp/#";

        // Seed → SDF reads MQTT continuously
        PCollection<byte[]> mqttStream = pipeline
                .apply("Seed", Create.of(brokerUrl))
                .apply("ReadFromMqtt", ParDo.of(new MqttSdfFn(brokerUrl, topicPattern, logMessages)));

        // Parse and transform
        PCollection<String> parsedStream = mqttStream.apply("ParsePayloads", ParDo.of(new DoFn<byte[], String>() {
            @ProcessElement
            public void processElement(@Element byte[] payload, OutputReceiver<String> out) {
                String payloadStr = new String(payload, StandardCharsets.UTF_8);
                JSONObject jsonObject = new JSONObject(payloadStr);
                JSONObject vp = jsonObject.optJSONObject("VP");

                if (vp != null) {
                    JSONObject renamedVp = KeyMapper.renameKeys(vp);
                    if (logMessages) {
                        LOG.info("RouteID: {} VehicleType: {}",
                                renamedVp.optString("RouteID", "N/A"),
                                renamedVp.optString("VehicleType", "N/A"));
                    }

                    JSONObject renamedVpWithSpeedKmh = new JSONObject(renamedVp.toString());
                    if (renamedVp.has("Speed")) {
                        Integer speedInMs = renamedVp.optInt("Speed", 0);
                        renamedVpWithSpeedKmh.put("Speed", KeyMapper.convertSpeedToKmh(speedInMs));
                    }

                    JSONObject finalVp = new JSONObject(renamedVpWithSpeedKmh.toString());
                    if (renamedVpWithSpeedKmh.has("OdometerReading")) {
                        Integer distanceInMeters = renamedVpWithSpeedKmh.optInt("OdometerReading", 0);
                        finalVp.put("OdometerReading", KeyMapper.convertDistanceToKm(distanceInMeters));
                    }

                    out.output(finalVp.toString());
                }
            }
        }));

        // Publish to Pub/Sub
        parsedStream.apply("PublishToPubSub", ParDo.of(new PublishToPubSubFn(projectId, topicId)));

        pipeline.run();
    }

    // ── Pub/Sub Sink ───────────────────────────────────────────────────────────
    static class PublishToPubSubFn extends DoFn<String, Void> {
        private final String projectId;
        private final String topicId;
        private transient Publisher publisher;

        PublishToPubSubFn(String projectId, String topicId) {
            this.projectId = projectId;
            this.topicId = topicId;
        }

        @Setup
        public void setup() throws IOException {
            publisher = Publisher.newBuilder(TopicName.of(projectId, topicId)).build();
        }

        @Teardown
        public void teardown() throws Exception {
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }

        @ProcessElement
        public void processElement(@Element String jsonString) {
            ByteString data = ByteString.copyFrom(jsonString, StandardCharsets.UTF_8);
            PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();

            ApiFutures.addCallback(publisher.publish(message), new ApiFutureCallback<String>() {
                @Override
                public void onFailure(Throwable t) {
                    LOG.error("Failed to publish: {}", t.getMessage());
                }
                @Override
                public void onSuccess(String messageId) {
                    LOG.info("Published message ID: {}", messageId);
                }
            }, MoreExecutors.directExecutor());
        }
    }
}
