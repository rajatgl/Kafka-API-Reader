package com.bridgelabz.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

object Producer extends App {
  val topicName = "test-1"

  val producerProperties = new Properties()

  producerProperties.setProperty(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
  )
  producerProperties.setProperty(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[IntegerSerializer].getName
  )
  producerProperties.setProperty(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName
  )
  val producer = new KafkaProducer[Int,String](producerProperties)
  producer.send(new ProducerRecord[Int,String](topicName,1,"Hello!"))
  producer.send(new ProducerRecord[Int,String](topicName,2,"Hello!1"))
  producer.send(new ProducerRecord[Int,String](topicName,3,"Hello!2"))
  producer.send(new ProducerRecord[Int,String](topicName,4,"Hello!3"))
  producer.send(new ProducerRecord[Int,String](topicName,5,"Hello!4"))

  producer.flush()

}