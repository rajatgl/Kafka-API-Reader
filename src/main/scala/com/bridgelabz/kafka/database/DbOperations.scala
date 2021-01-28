package com.bridgelabz.kafka.database

import akka.actor.ActorSystem
import com.bridgelabz.kafka.utils.Utilities.getAttribute
import redis.RedisClient
import spray.json._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

/**
 * Created on 1/28/2021.
 * Class: DbOperations.scala
 * Author: Rajat G.L.
 */
object DbOperations {

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()
  implicit val executionContext: ExecutionContext = akkaSystem.dispatcher

  val redis: RedisClient = RedisClient()

  def ping(): Unit = {

    val futurePong: Future[String] = redis.ping()
    println("Ping sent!")
    futurePong.map(pong => {
      println(s"Redis replied with a $pong")
    })
    Await.result(futurePong, 5.seconds)
  }

  def add(jsValue: JsValue): Future[Option[JsValue]] = {

    val futureAdded =
      redis.hmset(getAttribute("_id", jsValue).toString(),
        Map("id" -> getAttribute("_id", jsValue).toString(),
          "title" -> getAttribute("title", jsValue).toString(),
          "pdfLink" -> getAttribute("pdfLink", jsValue).toString()))

    futureAdded map {res => if(res) Some(jsValue) else None}
  }

  def find(id: String): Future[Option[JsValue]] = {
    redis.hgetall(id) map { values =>
      if (values.isEmpty) None else {
        val data =
          s"""
            {
              "id":"${values("id").toString()}",
              "title":"${values("title").toString()}",
              "pdfLink":"${values("pdfLink").toString()}"
            }
          """.stripMargin

        Some(data.parseJson)
      }
    }
  }
}
