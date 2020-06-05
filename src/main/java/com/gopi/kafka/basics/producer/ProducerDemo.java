/**
 * Created by gopinath_mb on 05-Jun-2020
 */
package com.gopi.kafka.basics.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author gopinath_mb
 */
public class ProducerDemo
{

  public static void main(String[] args) throws InterruptedException
  {
    // Create properties required for the producer
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    // Create Producer with given properties
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
        properties);
    for (int i = 1; i < 1000; i++)
    {
      Thread.sleep(1000);
      // Create Record of data which needs to be sent to the consumer.
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(
          "first_topic", "helloWorld from eclipse ==>"+i);
      // Send the record to producer
      producer.send(record);
    }
    // We must either flush or close producer else we may not be able to see the
    // data as send method is async and thread may get closed without producing
    // the data.
    // flush
    producer.flush();

    // flush and close
    producer.close();
  }

}
