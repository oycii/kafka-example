package com.github.oycii.kafka.producer

import com.github.oycii.kafka.producer.entity.Book
import com.typesafe.scalalogging.LazyLogging

import java.io.FileReader
import org.apache.commons.csv.{CSVFormat, CSVRecord}
import io.circe.syntax._

import scala.language.implicitConversions
import io.circe.generic.auto._
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization._
import shapeless.HList.ListCompat.::

import java.time.Duration
import scala.jdk.CollectionConverters.{asJavaCollection, asJavaCollectionConverter}
import java.util.Properties
import scala.::
import scala.collection.immutable.HashMap
import scala.collection.mutable

object Main extends LazyLogging  {
  val servers = "localhost:29092"
  val topic = "books"
  val group   = "g3"


  def getBook(rec: CSVRecord): Book = {
    Book(rec.get("Name"), rec.get("Author"), rec.get("User Rating"), rec.get("Reviews"), rec.get("Price"), rec.get("Year"), rec.get("Genre"))
  }

  def send: Unit = {
    val in = new FileReader("bestsellers-with-categories.csv")
    val records = CSVFormat.DEFAULT.withFirstRecordAsHeader.parse(in)

    val props = new Properties()
    props.put("bootstrap.servers", servers)
    val producer: KafkaProducer[String, String] = new KafkaProducer(props, new StringSerializer, new StringSerializer)
    try {
      val it = records.iterator
      while (it.hasNext) {
        val list = it.next()
        println("list: " + list)
        println("size: " + list.size)
        val book = getBook(list)
        val json = book.asJson
        println(json)
        producer.send(new ProducerRecord(topic, book.hashCode.toString, json.toString()))
      }
    } finally {
      producer.close()
      in.close()
    }
  }

  def addMessage(msg: ConsumerRecord[String, String], lastMessages: mutable.Map[Int, List[(Long, String)]]): Unit = {
    val partitionData: Option[List[(Long, String)]] = lastMessages.get(msg.partition())
    partitionData match {
      case Some(value) =>
        logger.info("size of partition: " + msg.partition() + " - " + value.size)
        if (value.size < 5) {
          lastMessages.put(msg.partition(), value ++ List((msg.offset(), msg.value())))
          logger.info("add for partition: " + msg.partition() + ", offset: " + msg.offset())
        } else {
          val minOffset = value.map(_._1).min
          if (msg.offset() > minOffset) {
            val newList: List[(Long, String)] = value.filter(rec => rec._1 != minOffset) ++ List((msg.offset(), msg.value()))
            lastMessages.put(msg.partition(), newList)
            logger.info("update for partition: " + msg.partition() + ", offset: " + msg.offset())
          }
        }

      case None => val listDataOfPartition = List((msg.offset(), msg.value()))
        lastMessages.put(msg.partition(), listDataOfPartition)
    }
  }

  def viewLastMessages(lastMessages: mutable.Map[Int, List[(Long, String)]]): Unit = {
    lastMessages.keys.foreach(key => {
      lastMessages.get(key).foreach(kv => kv.foreach(v => { logger.info("partition: " + key + ", offset: " + v._1 + ", msg: " + v._2) }))
    })
  }

  def consume: Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", servers)
    props.put("group.id", group)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", false)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)
    consumer.subscribe(List(topic).asJavaCollection)

    val parts = consumer.assignment()
    val lastMessages: mutable.Map[Int, List[(Long, String)]] = mutable.HashMap[Int, List[(Long, String)]]()

    // Читаем тему
    try {
      val timeout = 5000
      var lastTime = System.currentTimeMillis()
      var isNext = true
      while (isNext) {
        val v = consumer.poll(Duration.ofSeconds(1))
        if (v.count() != 0) {
          lastTime = System.currentTimeMillis()
          v.forEach { msg => {
              addMessage(msg, lastMessages)
            }
          }
        } else {
          if (System.currentTimeMillis() - lastTime > timeout) {
            viewLastMessages(lastMessages)
            lastMessages.clear()
            isNext = false
          }
        }
      }
    } catch {
      case e: Exception =>
        println(e.getLocalizedMessage)
        sys.exit(-1)
    } finally {
      consumer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    send
    consume
  }
}
