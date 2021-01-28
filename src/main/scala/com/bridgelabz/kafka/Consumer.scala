package com.bridgelabz.kafka

import java.time.Duration
import java.util
import java.util.{Date, Properties}

import com.bridgelabz.kafka.database.DbOperations
import com.bridgelabz.kafka.utils.Utilities.getAttribute
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object Consumer extends App {
  val topicName = "Magazines"
  val consumerProperties = new Properties
  consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
  consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroupID")
  val consumer = new KafkaConsumer[Int,String](consumerProperties)
  val topicNames = new util.ArrayList[String]
  topicNames.add(topicName)
  consumer.subscribe(topicNames)

  while(true)
  {
    val consumerPoll: ConsumerRecords[Int, String] = consumer.poll(Duration.ofSeconds(2))

    println(s"Polled at ${new Date().getTime} and found ${consumerPoll.count()} records.")
    val consumerIterator = consumerPoll.iterator()
    while (consumerIterator.hasNext)
    {
      val consumerRecords = consumerIterator.next()
      println(s"${consumerRecords.key()}: ${consumerRecords.value()}- Found at partition ${consumerRecords.partition()} and offset ${consumerRecords.offset()}")

      val added = DbOperations.add(consumerRecords.value().parseJson)
      val result = Await.result(added, 60.seconds)

      if(result.isDefined){
        println(s"${getAttribute("title", result.get)} added to the database.")
      }
      else{
        println("ERROR ADDING TO DB.")
      }
    }
  }
}
