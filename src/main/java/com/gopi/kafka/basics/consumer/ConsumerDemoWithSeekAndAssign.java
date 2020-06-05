/**
 * Created by gopinath_mb on 05-Jun-2020
 */
package com.gopi.kafka.basics.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gopinath_mb
 */
public class ConsumerDemoWithSeekAndAssign
{
  private static final Logger LOGGER = LoggerFactory
      .getLogger(ConsumerDemoWithSeekAndAssign.class);

  public static void main(String[] args)
  {

    // Set all the required properties
    Properties properties = new Properties();
    String bootStrapServers = "127.0.0.1:9092";
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootStrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Create Kakfa consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
        properties);

    TopicPartition topicPartition = new TopicPartition("first_topic", 0);
    // assign which topic and partition you want to read from

    consumer.assign(Arrays.asList(topicPartition));

    // Tell the consumer about offset value from where you wanted to read from.
    consumer.seek(topicPartition, 5);

    LOGGER.info("Started to read messages..");
    int numOfMessagesToBeRead=5;
    int numOfMessagesRead=0;
    while (numOfMessagesRead < numOfMessagesToBeRead)
    {

      ConsumerRecords<String, String> records = consumer
          .poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> consumerRecord : records)
      {
        numOfMessagesRead++;
        LOGGER.info(" Key: " + consumerRecord.key() + " Topic: "
            + consumerRecord.topic() + " Partition: "
            + consumerRecord.partition() + " Data: " + consumerRecord.value()
            + " Offset: " + consumerRecord.offset());
      }
    }
    
    LOGGER.info("Completed reading "+numOfMessagesToBeRead+" number of messages.");

  }
}
