package com.bridgelabz.kafka.utils

import spray.json._

/**
 * Created on 1/28/2021.
 * Class: Utilities.scala
 * Author: Rajat G.L.
 */

object Utilities {
  /**
   *
   * @param url to fetch (GET) the data from
   * @return JsValue of the data fetched
   */
  def getData(url :String): Option[JsValue] ={

    val source = scala.io.Source.fromURL(url)

    try {
      val data = source.mkString
      Option(data.parseJson)
    }
    catch{
      case _: Throwable =>
        println(s"Error parsing URL: ${url}")
        None
    }
    finally {
      source.close()
    }
  }
}
