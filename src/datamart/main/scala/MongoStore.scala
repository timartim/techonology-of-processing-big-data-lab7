package ru.bigdata.datamart

import io.circe.Json
import org.mongodb.scala._
import org.mongodb.scala.bson.collection.immutable.Document

import java.time.Instant
import scala.concurrent.Await
import scala.concurrent.duration._

final class MongoStore(mongoUri: String, databaseName: String) {
  private val client = MongoClient(mongoUri)
  private val database = client.getDatabase(databaseName)
  private val timeout = 60.seconds

  def saveTrainingResults(request: Json): Unit = {
    val runId = Instant.now().toString

    replaceJsonList("cluster_profiles", request, "profiles")
    replaceJsonList("cluster_centers", request, "centers")
    replaceJsonDocument("training_metrics", field(request, "metrics"), runId)
    replaceJsonDocument("model_info", field(request, "modelInfo"), runId)
  }

  def close(): Unit = client.close()

  private def list(json: Json, name: String): List[Json] =
    json.hcursor.downField(name).as[List[Json]].getOrElse(Nil)

  private def field(json: Json, name: String): Json =
    json.hcursor.downField(name).as[Json].getOrElse(Json.obj())

  private def replaceJsonList(collectionName: String, json: Json, fieldName: String): Unit =
    replace(collectionName, list(json, fieldName).map(toDocument))

  private def replaceJsonDocument(collectionName: String, json: Json, runId: String): Unit =
    replace(collectionName, List(withRunId(json, runId)))

  private def replace(name: String, docs: List[Document]): Unit = {
    val collection = database.getCollection(name)
    await(collection.deleteMany(Document()))
    if (docs.nonEmpty) await(collection.insertMany(docs))
  }

  private def withRunId(json: Json, runId: String): Document =
    toDocument(json) + ("run_id" -> runId)

  private def toDocument(json: Json): Document =
    Document(json.noSpaces)

  private def await[T](observable: SingleObservable[T]): T =
    Await.result(observable.toFuture(), timeout)
}
