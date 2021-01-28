name := "KafkaProducerConsumer"

version := "0.1"

scalaVersion := "2.13.4"

libraryDependencies += "org.apache.kafka"%"kafka-clients"%"2.7.0"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.6"
libraryDependencies += "com.github.etaty" %% "rediscala" % "1.9.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.32"