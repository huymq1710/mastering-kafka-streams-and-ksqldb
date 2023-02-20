package com.magicalpipelines.serialization.json;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.magicalpipelines.serialization.Tweet;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Deserializer;

public class TweetDeserializer implements Deserializer<Tweet> {

  // Twitter Kafka connector uses upper camel case for field names (VD: ThisIsAnExample)
  private Gson gson =
      new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();

  // Override the deserialize method with our own logic for deserializing records in the tweets
  // topic
  @Override
  public Tweet deserialize(String topic, byte[] bytes) {
    if (bytes == null) return null;

    // Use the Gson library to deserialize the byte array into a Tweet object
    return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), Tweet.class);
  }
}
