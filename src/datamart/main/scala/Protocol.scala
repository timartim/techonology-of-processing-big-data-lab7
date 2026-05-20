package ru.bigdata.datamart

import io.circe.Json
import io.circe.parser.parse

object Protocol {
  val Status = "status"
  val Data = "data"
  val Error = "error"

  val Rows = "rows"
  val FeatureCols = "featureCols"
  val ProductCols = "productCols"
  val TotalRows = "totalRows"
  val WorkingRows = "workingRows"

  val InputPath = "inputPath"
  val MinNonNullRatio = "minNonNullRatio"
  val TargetRows = "targetRows"
  val Seed = "seed"

  val Profiles = "profiles"
  val Centers = "centers"
  val Metrics = "metrics"
  val ModelInfo = "modelInfo"

  def parseBody(body: String): Json =
    if (body.trim.isEmpty) Json.obj() else parse(body).fold(throw _, identity)

  def parseJson(text: String): Option[Json] =
    parse(text).toOption

  def ok(data: Json): Json =
    Json.obj(Status -> Json.fromString("ok"), Data -> data)

  def error(message: String): Json =
    Json.obj(Status -> Json.fromString("error"), Error -> Json.fromString(message))
}
