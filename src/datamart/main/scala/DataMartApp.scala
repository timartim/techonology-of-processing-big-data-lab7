package ru.bigdata.datamart

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.circe.Json
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
import java.util.concurrent.{CountDownLatch, Executors}
import scala.util.control.NonFatal
import Protocol._

object DataMartApp {
  private val config = DataMartConfig.load()

  private val spark = SparkSession
    .builder()
    .appName(config.spark.appName)
    .master(config.spark.master)
    .config("spark.driver.memory", config.spark.driverMemory)
    .config("spark.driver.cores", config.spark.driverCores)
    .config("spark.executor.memory", config.spark.executorMemory)
    .config("spark.executor.cores", config.spark.executorCores)
    .config("spark.executor.memoryOverhead", config.spark.memoryOverhead)
    .config("spark.driver.maxResultSize", config.spark.maxResultSize)
    .config("spark.default.parallelism", config.spark.defaultParallelism)
    .config("spark.sql.shuffle.partitions", config.spark.shufflePartitions)
    .config("spark.sql.adaptive.enabled", config.spark.adaptiveEnabled)
    .getOrCreate()

  spark.sparkContext.setLogLevel(config.spark.logLevel)

  private val store = new MongoStore(
    config.mongo.uri,
    config.mongo.database
  )

  def main(args: Array[String]): Unit = {
    val server = HttpServer.create(new InetSocketAddress(config.server.host, config.server.port), 0)
    server.setExecutor(Executors.newFixedThreadPool(4))

    route(server, "/v1/training-data") { body =>
      prepareTrainingData(parseBody(body))
    }

    route(server, "/v1/training-results") { body =>
      store.saveTrainingResults(parseBody(body))
      println("Training results saved to MongoDB")
      Json.obj("saved" -> Json.fromBoolean(true))
    }

    sys.addShutdownHook {
      server.stop(0)
      store.close()
      spark.stop()
    }
    server.start()
    println(s"Data mart is listening on ${config.server.host}:${config.server.port}")
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
    val rows = sampled.toJSON.collect().flatMap(parseJson)

    Json.obj(
      Rows -> Json.arr(rows: _*),
      FeatureCols -> Json.arr(featureColumns.map(Json.fromString): _*),
      ProductCols -> Json.arr(productCols.map(Json.fromString): _*),
      TotalRows -> Json.fromLong(totalRows),
      WorkingRows -> Json.fromInt(rows.length)
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
        inputPath = text(json, InputPath, "../data/food_small.parquet"),
        minNonNullRatio = number(json, MinNonNullRatio, 0.9),
        targetRows = int(json, TargetRows, 100000),
        seed = int(json, Seed, 42).toLong
      )
  }

  private def resolve(path: String): String = {
    val p = Paths.get(path)
    if (p.isAbsolute) p.toString else Path.of(sys.props("user.dir")).resolve(p).normalize().toString
  }

  private def isNumeric(t: DataType): Boolean = t.isInstanceOf[NumericType]

  private def text(json: Json, name: String, default: String): String = json.hcursor.downField(name).as[String].getOrElse(default)

  private def int(json: Json, name: String, default: Int): Int = json.hcursor.downField(name).as[Int].getOrElse(default)

  private def number(json: Json, name: String, default: Double): Double = json.hcursor.downField(name).as[Double].getOrElse(default)

  private def write(exchange: HttpExchange, code: Int, json: Json): Unit = {
    val bytes = json.noSpaces.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(code, bytes.length)
    exchange.getResponseBody.write(bytes)
  }

  private final case class DataMartConfig(
      server: ServerConfig,
      spark: SparkConfig,
      mongo: MongoConfig
  )

  private final case class ServerConfig(host: String, port: Int)

  private final case class SparkConfig(
      appName: String,
      master: String,
      driverMemory: String,
      driverCores: String,
      executorMemory: String,
      executorCores: String,
      memoryOverhead: String,
      maxResultSize: String,
      defaultParallelism: String,
      shufflePartitions: String,
      adaptiveEnabled: String,
      logLevel: String
  )

  private final case class MongoConfig(uri: String, database: String)

  private object DataMartConfig {
    def load(): DataMartConfig = {
      val path = sys.env.getOrElse("DATAMART_CONFIG", "config.json")
      val json = parseBody(scala.io.Source.fromFile(path, "UTF-8").mkString)

      DataMartConfig(
        server = ServerConfig(
          host = env("DATAMART_HOST", textAt(json, "server", "host", "0.0.0.0")),
          port = env("DATAMART_PORT", intAt(json, "server", "port", 8090).toString).toInt
        ),
        spark = SparkConfig(
          appName = env("DATAMART_SPARK_APP_NAME", textAt(json, "spark", "app_name", "OpenFoodFacts-DataMart")),
          master = env("DATAMART_SPARK_MASTER", textAt(json, "spark", "master", "local[2]")),
          driverMemory = env("DATAMART_SPARK_DRIVER_MEMORY", textAt(json, "spark", "driver_memory", "1g")),
          driverCores = env("DATAMART_SPARK_DRIVER_CORES", textAt(json, "spark", "driver_cores", "1")),
          executorMemory = env("DATAMART_SPARK_EXECUTOR_MEMORY", textAt(json, "spark", "executor_memory", "1g")),
          executorCores = env("DATAMART_SPARK_EXECUTOR_CORES", textAt(json, "spark", "executor_cores", "1")),
          memoryOverhead = env("DATAMART_SPARK_MEMORY_OVERHEAD", textAt(json, "spark", "memory_overhead", "256m")),
          maxResultSize = env("DATAMART_SPARK_MAX_RESULT_SIZE", textAt(json, "spark", "max_result_size", "256m")),
          defaultParallelism = env("DATAMART_SPARK_DEFAULT_PARALLELISM", textAt(json, "spark", "default_parallelism", "4")),
          shufflePartitions = env("DATAMART_SPARK_SHUFFLE_PARTITIONS", textAt(json, "spark", "shuffle_partitions", "4")),
          adaptiveEnabled = env("DATAMART_SPARK_ADAPTIVE_ENABLED", textAt(json, "spark", "adaptive_enabled", "true")),
          logLevel = env("DATAMART_SPARK_LOG_LEVEL", textAt(json, "spark", "log_level", "WARN"))
        ),
        mongo = MongoConfig(
          uri = requiredTextAt(json, "mongo", "uri"),
          database = requiredTextAt(json, "mongo", "database")
        )
      )
    }

    private def env(name: String, default: String): String =
      sys.env.getOrElse(name, default)

    private def textAt(json: Json, section: String, name: String, default: String): String =
      json.hcursor.downField(section).downField(name).as[String].getOrElse(default)

    private def intAt(json: Json, section: String, name: String, default: Int): Int =
      json.hcursor.downField(section).downField(name).as[Int].getOrElse(default)

    private def requiredTextAt(json: Json, section: String, name: String): String =
      json.hcursor.downField(section).downField(name).as[String].getOrElse {
        throw new IllegalArgumentException(s"Missing config value: $section.$name")
      }
  }
}
