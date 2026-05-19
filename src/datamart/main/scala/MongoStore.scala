package ru.technology-of-processing-big-data.datamart

import io.circe.Json
import io.circe.parser.parse
import org.bson.Document
import org.mongodb.scala._
import org.mongodb.scala.model.Projections

import java.time.Instant
import scala.concurrent.Await
import scala.concurrent.duration._

final class MongoStore(mongoUri: String, databaseName: String) {
  private val client = MongoClient(mongoUri)
  private val database = client.getDatabase(databaseName)
  private val timeout = 60.seconds

  def queryTrainingData(request: Json): List[Json] = {
    val fields = request.hcursor.downField("fields").as[List[String]].getOrElse(Nil)
    val filterJson = request.hcursor.downField("filters").as[Json].getOrElse(Json.obj())
    val limit = request.hcursor.downField("limit").as[Int].getOrElse(0)

    val projection =
      if (fields.isEmpty) Projections.excludeId()
      else Projections.fields((fields.map(field => Projections.include(field)) :+ Projections.excludeId()): _*)

    val docs = awaitMany(
      collection("training_data")
        .find(toDocument(filterJson))
        .projection(projection)
        .limit(limit)
    )

    docs.toList.flatMap(fromDocument)
  }

  def saveTrainingResults(request: Json): Unit = {
    val runId = Instant.now().toString

    replace("training_clusters", jsonList(request, "clusters").map(toDocument))
    replace("cluster_profiles", jsonList(request, "profiles").map(toDocument))
    replace("cluster_centers", jsonList(request, "centers").map(toDocument))
    replace("training_metrics", List(withRunId(jsonField(request, "metrics"), runId)))
    replace("model_info", List(withRunId(jsonField(request, "modelInfo"), runId)))
  }

  def savePredictions(request: Json): Long = {
    val runId = Instant.now().toString
    val sourcePath = request.hcursor.downField("sourcePath").as[String].toOption

    val docs = jsonList(request, "predictions").map { row =>
      val doc = toDocument(row).append("run_id", runId)
      sourcePath.foreach(path => doc.append("source_path", path))
      doc
    }

    replace("predictions", docs)
    docs.size.toLong
  }

  def close(): Unit = client.close()

  private def jsonList(json: Json, field: String): List[Json] =
    json.hcursor.downField(field).as[List[Json]].getOrElse(Nil)

  private def jsonField(json: Json, field: String): Json =
    json.hcursor.downField(field).as[Json].getOrElse(Json.obj())

  private def collection(name: String): MongoCollection[Document] =
    database.getCollection(name)

  private def replace(name: String, docs: List[Document]): Unit = {
    val target = collection(name)
    awaitOne(target.deleteMany(new Document()))
    if (docs.nonEmpty) {
      awaitOne(target.insertMany(docs))
    }
  }

  private def withRunId(json: Json, runId: String): Document =
    toDocument(json).append("run_id", runId)

  private def toDocument(json: Json): Document =
    Document.parse(json.noSpaces)

  private def fromDocument(document: Document): Option[Json] =
    parse(document.toJson()).toOption

  private def awaitOne[T](observable: SingleObservable[T]): T =
    Await.result(observable.toFuture(), timeout)

  private def awaitMany[T](observable: Observable[T]): Seq[T] =
    Await.result(observable.toFuture(), timeout)
}
