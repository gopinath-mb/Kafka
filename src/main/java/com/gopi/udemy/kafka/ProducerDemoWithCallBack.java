/**
 * Created With Love By Gopi on 08-Mar-2020
 */
package com.gopi.udemy.kafka;

import java.util.Properties;

import org.apache.commons.math3.analysis.function.Log;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Gopi
 */
public class ProducerDemoWithCallBack
{
	private static Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
	

	public static void main(String[] args)
	{

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 20; i++)
		{
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "helloWorld "+i);

			producer.send(record, new Callback()
			{
				// whether it pass or fail it will come here
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception)
				{

					if (null == exception)
					{

						LOGGER.info("Topic: " + metadata.topic() + " Partition: " + metadata.partition() + " Off-Set: "
								+ metadata.offset() + " timestamp: " + metadata.timestamp());
					} else
					{
						LOGGER.error("Exception occurred while Consuming: ", exception);
					}

				}
			});
		}
		// flush
		producer.flush();

		// flush and close
		producer.close();
	}

}
