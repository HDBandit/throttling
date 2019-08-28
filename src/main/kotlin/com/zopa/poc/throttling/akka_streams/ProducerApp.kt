package com.zopa.poc.throttling.akka_streams

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.javadsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.javadsl.Source
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration

fun main() {
    val config = ConfigFactory.load()
    val system = ActorSystem.create("Producer", config)
    val materializer = ActorMaterializer.create(system)

    val producerSettings = ProducerSettings.create(
        system.settings().config().getConfig("akka.kafka.producer"),
        StringSerializer(),
        StringSerializer()
    )
        .withBootstrapServers("localhost:9092")
        .withProperty("max.in.flight.requests.per.connection", "1")

    val topic = "test-topic"

    val done1 = Source.range(1, 100)
        .map { number -> ProducerRecord<String, String>(topic, number.toString(), "MSG $number") }
//        .throttle(1, Duration.ofSeconds(1))
        .runWith(Producer.plainSink(producerSettings), materializer)

    done1.thenRun { system.terminate() }
}