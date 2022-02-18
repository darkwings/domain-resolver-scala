package com.nttdata.poc.model

import com.google.gson.annotations.SerializedName
import com.google.gson.{FieldNamingPolicy, Gson, GsonBuilder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

import java.nio.charset.StandardCharsets

case class Location(@(SerializedName @scala.annotation.meta.field)("CITY") city: String,
                    @(SerializedName @scala.annotation.meta.field)("STATE") state: String,
                    @(SerializedName @scala.annotation.meta.field)("LATITUDE") latitude: Double,
                    @(SerializedName @scala.annotation.meta.field)("LONGITUDE") Longitude: Double)

case class Domain(domain: String, suspect: Boolean, timestamp: String)

case class Activity(@(SerializedName @scala.annotation.meta.field)("ACTIVITYID") activityId: String,
                    @(SerializedName @scala.annotation.meta.field)("USERID") userId: String,
                    @(SerializedName @scala.annotation.meta.field)("CN") commonName: String,
                    @(SerializedName @scala.annotation.meta.field)("ROOMNUMBER")roomNumber: String,
                    @(SerializedName @scala.annotation.meta.field)("EMAIL") email: String,
                    @(SerializedName @scala.annotation.meta.field)("IP") ip: String,
                    @(SerializedName @scala.annotation.meta.field)("DOMAIN") domain: String,
                    @(SerializedName @scala.annotation.meta.field)("LOCATION") location: Location,
                    @(SerializedName @scala.annotation.meta.field)("MESSAGE") message: String,
                    @(SerializedName @scala.annotation.meta.field)("ACTION") action:String)

case class ActivityEnriched(@(SerializedName @scala.annotation.meta.field)("ACTIVITY") activity: Activity,
                            @(SerializedName @scala.annotation.meta.field)("SUSPECT") suspect: Boolean)


object JsonSerDes {

  def activity(): Serde[Activity] = {
    val serializer = new JsonSerializer[Activity]
    val deserializer = new JsonDeserializer[Activity](classOf[Activity])
    Serdes.serdeFrom(serializer, deserializer)
  }

  def activityEnriched(): Serde[ActivityEnriched] = {
    val serializer = new JsonSerializer[ActivityEnriched]
    val deserializer = new JsonDeserializer[ActivityEnriched](classOf[ActivityEnriched])
    Serdes.serdeFrom(serializer, deserializer)
  }

  def domain(): Serde[Domain] = {
    val serializer = new JsonSerializer[Domain]
    val deserializer = new JsonDeserializer[Domain](classOf[Domain])
    Serdes.serdeFrom(serializer, deserializer)
  }
}

class JsonSerializer[T] extends Serializer[T] {
  val gson: Gson = new GsonBuilder()
    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create

  override def serialize(topic: String, data: T): Array[Byte] =
    gson.toJson(data).getBytes(StandardCharsets.UTF_8)
}

class JsonDeserializer[T](destinationClass: Class[T]) extends Deserializer[T] {
  val gson: Gson = new GsonBuilder()
    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create

  override def deserialize(topic:String,bytes: Array[Byte]): T = {
    gson.fromJson(new String(bytes, StandardCharsets.UTF_8), destinationClass)
  }
}

