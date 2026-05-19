package ru.technology-of-processing-big-data.datamart

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.circe.Json
import io.circe.parser.parse

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import scala.util.control.NonFatal

object DataMartApp {
  def main(args: Array[String]): Unit = {
    val host = sys.env.getOrElse("DATAMART_HOST", "0.0.0.0")
    val port = sys.env.get("DATAMART_PORT").flatMap(_.toIntOption).getOrElse(8090)
    val mongoUri = sys.env.getOrElse("MONGO_URI", "mongodb://localhost:27017/openfoodfacts")
    val database = sys.env.getOrElse("MONGO_DATABASE", "openfoodfacts")

    val store = new MongoStore(mongoUri, database)
    val server = HttpServer.create(new InetSocketAddress(host, port), 0)

    route(server, "/health", "GET") { _ =>
      Json.obj("service" -> Json.fromString("datamart"))
    }

    route(server, "/v1/source/training/query", "POST") { body =>
      Json.obj("rows" -> Json.arr(store.queryTrainingData(readJson(body)): _*))
    }

    route(server, "/v1/model-results/training", "POST") { body =>
      store.saveTrainingResults(readJson(body))
      Json.obj("saved" -> Json.fromBoolean(true))
    }

    route(server, "/v1/model-results/predictions", "POST") { body =>
      Json.obj("rows" -> Json.fromLong(store.savePredictions(readJson(body))))
    }

    sys.addShutdownHook {
      server.stop(0)
      store.close()
    }
    server.start()
    println(s"Data mart is listening on $host:$port")

    new CountDownLatch(1).await()
  }

  private def route(server: HttpServer, path: String, method: String)(action: String => Json): Unit = {
    server.createContext(path, exchange => {
      try {
        if (exchange.getRequestMethod != method) {
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
    })
  }

  private def readJson(body: String): Json =
    if (body.trim.isEmpty) Json.obj() else parse(body).fold(throw _, identity)

  private def ok(data: Json): Json =
    Json.obj("status" -> Json.fromString("ok"), "data" -> data)

  private def error(message: String): Json =
    Json.obj("status" -> Json.fromString("error"), "error" -> Json.fromString(message))

  private def write(exchange: HttpExchange, code: Int, json: Json): Unit = {
    val bytes = json.noSpaces.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(code, bytes.length)
    exchange.getResponseBody.write(bytes)
  }
}
