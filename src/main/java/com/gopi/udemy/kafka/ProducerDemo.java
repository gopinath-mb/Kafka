/**
 * Created With Love By Gopi on 08-Mar-2020
 */
package com.gopi.udemy.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author Gopi
 */
public class ProducerDemo
{

	public static void main(String[] args)
	{

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "helloWorld2");
		
		producer.send(record);
		
		//flush
		producer.flush();
		
		//flush and close
		producer.close();
	}

}
