package com.test.flink.exactly.mysql;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 模拟产生数据，发送到Kafka Topic中
 */
public class MockKafkaDemo {

	public static void main(String[] args) throws Exception {
		// 1. Kafka Producer 生产者属性配置
		Properties props = new Properties();
		props.put("bootstrap.servers", "node1:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 2. 构建Producer对象
		Producer<String, String> producer = new KafkaProducer<>(props);
		// 3. 构建Record对象
		for (int i = 1; i <= 200; i++) {
			String message = "val_" + i ;
			System.out.println("Message>>>>>>" + message);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(
				"mysql-topic", message
			);
			producer.send(record);
			// 休眠1秒
			Thread.sleep(1000);
		}
		// 4. 数据刷新
		producer.flush();
	}

}
