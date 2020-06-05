/**
 * Created by gopinath_mb on 05-Jun-2020
 */
package com.gopi.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gopinath_mb
 */
//We used sync send method (get()) to understand keys and u should never do that.
public class ProducerDemoWithKeys
{

  private static Logger LOGGER = LoggerFactory
      .getLogger(ProducerDemoWithCallBack.class);

  public static void main(String[] args)
      throws InterruptedException, ExecutionException
  {

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
        properties);

    for(int i = 0; i < 500; i++)
    {
      Thread.sleep(1000);
      for (int j = 0; j < 5; j++)
      {
        String topic = "first_topic";
        String value = "helloWorld from Eclipse with Callback =>" + i;
        String key = "id_" + j;

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
            topic, key, value);
        LOGGER.info("Key is :" + key);
        producer.send(record, new Callback()
        {
          // whether it pass or fail it will come here
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception)
          {

            if (null == exception)
            {

              LOGGER.info("Topic: " + metadata.topic() + " Partition: "
                  + metadata.partition() + " Off-Set: " + metadata.offset()
                  + " timestamp: " + metadata.timestamp());
            } else
            {
              LOGGER.error("Exception occurred while Consuming: ", exception);
            }

          }
        }).get(); //Never make kafka sync, this is just for understanding. 
      }
    }

// flush
    producer.flush();

// flush and close
    producer.close();
  }

}
