package com.bridgelabz.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

object Producer extends App {
  val topicName = "Magazines"

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

  var key: Int = 0
  for(magazine <- utils.Utilities.getMagazines.get){
    producer.send(new ProducerRecord[Int,String](topicName, key, magazine.toString()))
    key += 1
  }

  producer.flush()

}