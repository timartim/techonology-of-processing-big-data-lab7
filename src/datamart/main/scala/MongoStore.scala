package ru.bigdata.datamart

import io.circe.Json
import com.mongodb.client.MongoClients
import org.bson.Document

import java.time.Instant
import scala.jdk.CollectionConverters._
import Protocol._

final class MongoStore(mongoUri: String, databaseName: String) {
  private val client = MongoClients.create(mongoUri)
  private val database = client.getDatabase(databaseName)

  def saveTrainingResults(request: Json): Unit = {
    val runId = Instant.now().toString

    replaceJsonList("cluster_profiles", request, Profiles)
    replaceJsonList("cluster_centers", request, Centers)
    replaceJsonDocument("training_metrics", field(request, Metrics), runId)
    replaceJsonDocument("model_info", field(request, ModelInfo), runId)
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
    collection.deleteMany(new Document())
    if (docs.nonEmpty) collection.insertMany(docs.asJava)
  }

  private def withRunId(json: Json, runId: String): Document =
    toDocument(json).append("run_id", runId)

  private def toDocument(json: Json): Document =
    Document.parse(json.noSpaces)
}
