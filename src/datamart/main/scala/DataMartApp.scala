package ru.bigdata.datamart

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
import java.util.concurrent.CountDownLatch
import scala.util.control.NonFatal

object DataMartApp {
  private val spark = SparkSession
    .builder()
    .appName("OpenFoodFacts-DataMart")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  private val store = new MongoStore(
    sys.env.getOrElse("MONGO_URI", "mongodb://localhost:27017/openfoodfacts"),
    sys.env.getOrElse("MONGO_DATABASE", "openfoodfacts")
  )

  def main(args: Array[String]): Unit = {
    val host = sys.env.getOrElse("DATAMART_HOST", "0.0.0.0")
    val port = sys.env.get("DATAMART_PORT").flatMap(_.toIntOption).getOrElse(8090)
    val server = HttpServer.create(new InetSocketAddress(host, port), 0)

    route(server, "/v1/training-data") { body =>
      prepareTrainingData(readJson(body))
    }

    route(server, "/v1/training-results") { body =>
      store.saveTrainingResults(readJson(body))
      Json.obj("saved" -> Json.fromBoolean(true))
    }

    sys.addShutdownHook {
      server.stop(0)
      store.close()
      spark.stop()
    }
    server.start()
    println(s"Data mart is listening on $host:$port")
    new CountDownLatch(1).await()
  }

  private def prepareTrainingData(request: Json): Json = {
    val requestData = TrainingDataRequest.fromJson(request)
    val raw = spark.read.parquet(resolve(requestData.inputPath))
    val totalRows = raw.count()

    if (totalRows == 0) throw new IllegalArgumentException("Input parquet is empty")

    val numericColumns = raw.schema.fields.collect {
      case f if isNumeric(f.dataType) => f.name
    }.toList
    if (numericColumns.isEmpty) throw new IllegalArgumentException("No numeric columns in input parquet")

    val filledColumns = keepColumnsWithEnoughValues(raw, numericColumns, totalRows, requestData.minNonNullRatio)
    val featureColumns = dropConstantColumns(raw, filledColumns)

    if (featureColumns.isEmpty) {
      throw new IllegalArgumentException("No numeric feature columns after preprocessing")
    }

    val (productExprs, productCols) = productColumns(raw.schema)

    val prepared = raw
      .select((productExprs ++ featureColumns.map(toDoubleColumn)): _*)
      .dropDuplicates()

    val sampled = sampleIfNeeded(prepared, requestData.targetRows, requestData.seed)
    val rows = sampled.toJSON.collect().flatMap(parse(_).toOption)

    Json.obj(
      "rows" -> Json.arr(rows: _*),
      "featureCols" -> Json.arr(featureColumns.map(Json.fromString): _*),
      "productCols" -> Json.arr(productCols.map(Json.fromString): _*),
      "totalRows" -> Json.fromLong(totalRows),
      "workingRows" -> Json.fromInt(rows.length)
    )
  }

  private def keepColumnsWithEnoughValues(
      df: org.apache.spark.sql.DataFrame,
      columns: List[String],
      totalRows: Long,
      minRatio: Double
  ): List[String] = {
    val expressions = columns.map(name => count(col(name)).alias(name))
    val counts = df.agg(expressions.head, expressions.tail: _*).first()

    val result = columns.filter { name =>
      counts.getAs[Long](name).toDouble / totalRows >= minRatio
    }

    if (result.isEmpty) {
      throw new IllegalArgumentException("No numeric columns with enough non-null values")
    }

    result
  }

  private def dropConstantColumns(
      df: org.apache.spark.sql.DataFrame,
      columns: List[String]
  ): List[String] = {
    val expressions = columns.map(name => stddev_samp(col(name)).alias(name))
    val stddevs = df.agg(expressions.head, expressions.tail: _*).first()

    columns.filter { name =>
      Option(stddevs.getAs[Double](name)).exists(_ > 0.0)
    }
  }

  private def sampleIfNeeded(
      df: org.apache.spark.sql.DataFrame,
      targetRows: Int,
      seed: Long
  ): org.apache.spark.sql.DataFrame = {
    val rowCount = df.count()

    if (rowCount > targetRows) {
      df.sample(withReplacement = false, targetRows.toDouble / rowCount, seed).limit(targetRows)
    } else {
      df
    }
  }

  private def toDoubleColumn(name: String): Column =
    col(name).cast("double").alias(name)

  private def productColumns(schema: StructType): (List[Column], List[String]) = {
    if (!schema.fieldNames.contains("product_name")) return (Nil, Nil)

    schema("product_name").dataType match {
      case ArrayType(StructType(fields), _) =>
        productColumnsFromArray(fields)

      case StructType(fields) =>
        val pairs = fields.toList.map { field =>
          val outputName = s"product_name_${field.name}"
          col(s"product_name.${field.name}").cast("string").alias(outputName) -> outputName
        }
        (pairs.map(_._1), pairs.map(_._2))

      case _ =>
        (List(col("product_name").cast("string").alias("product_name")), List("product_name"))
    }
  }

  private def productColumnsFromArray(fields: Array[StructField]): (List[Column], List[String]) = {
    val names = fields.map(_.name).toSet

    val pairs = List(
      Option.when(names.contains("lang"))(
        concat_ws(" | ", expr("transform(product_name, x -> x.lang)"))
          .alias("product_name_langs") -> "product_name_langs"
      ),
      Option.when(names.contains("text"))(
        concat_ws(" | ", expr("transform(product_name, x -> x.text)"))
          .alias("product_name_texts") -> "product_name_texts"
      )
    ).flatten

    (pairs.map(_._1), pairs.map(_._2))
  }

  private def route(server: HttpServer, path: String)(action: String => Json): Unit = {
    server.createContext(
      path,
      exchange => {
        try {
          if (exchange.getRequestMethod != "POST") {
            write(exchange, 405, error("Method not allowed"))
          } else {
            val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
            write(exchange, 200, ok(action(body)))
          }
        } catch {
          case NonFatal(e) => write(exchange, 400, error(e.getMessage))
        } finally {
          exchange.close()
        }
      }
    )
  }

  private final case class TrainingDataRequest(
      inputPath: String,
      minNonNullRatio: Double,
      targetRows: Int,
      seed: Long
  )

  private object TrainingDataRequest {
    def fromJson(json: Json): TrainingDataRequest =
      TrainingDataRequest(
        inputPath = text(json, "inputPath", "../data/food_small.parquet"),
        minNonNullRatio = number(json, "minNonNullRatio", 0.9),
        targetRows = int(json, "targetRows", 100000),
        seed = int(json, "seed", 42).toLong
      )
  }

  private def resolve(path: String): String = {
    val p = Paths.get(path)
    if (p.isAbsolute) p.toString else Path.of(sys.props("user.dir")).resolve(p).normalize().toString
  }

  private def isNumeric(t: DataType): Boolean = t.isInstanceOf[NumericType]

  private def readJson(body: String): Json = if (body.trim.isEmpty) Json.obj() else parse(body).fold(throw _, identity)

  private def text(json: Json, name: String, default: String): String = json.hcursor.downField(name).as[String].getOrElse(default)

  private def int(json: Json, name: String, default: Int): Int = json.hcursor.downField(name).as[Int].getOrElse(default)

  private def number(json: Json, name: String, default: Double): Double = json.hcursor.downField(name).as[Double].getOrElse(default)

  private def ok(data: Json): Json = Json.obj("status" -> Json.fromString("ok"), "data" -> data)

  private def error(message: String): Json = Json.obj("status" -> Json.fromString("error"), "error" -> Json.fromString(message))

  private def write(exchange: HttpExchange, code: Int, json: Json): Unit = {
    val bytes = json.noSpaces.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(code, bytes.length)
    exchange.getResponseBody.write(bytes)
  }
}
