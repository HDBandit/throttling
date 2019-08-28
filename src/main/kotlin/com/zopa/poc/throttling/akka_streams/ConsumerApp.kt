package com.zopa.poc.throttling.akka_streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.kafka.javadsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.javadsl.Sink
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

fun main() {
    class SFClient {
        fun send(records: List<ConsumerRecord<String, String>>): Done {
            System.out.println(">>>>>>> Batch of elements send to SF. Batch size: ${records.size}")
            return Done.getInstance()
        }
    }

    val config = ConfigFactory.load()
    val system = ActorSystem.create("Consumer", config)
    val materializer = ActorMaterializer.create(system)

    val sfClient = SFClient()

    val consumerSettings = ConsumerSettings.create(system, StringDeserializer(), StringDeserializer())
        .withBootstrapServers("localhost:9092")
        .withGroupId("group1")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val done = Consumer.plainSource(
        consumerSettings,
        Subscriptions.assignmentWithOffset(TopicPartition("test-topic", 0), 0)
    )
        .grouped(10)
        .throttle(1, Duration.ofSeconds(1))
        .map { record -> sfClient.send(record) }
        .runWith(Sink.ignore(), materializer)

    done.thenRun { system.terminate() }
}