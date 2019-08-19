package com.gelerion.spark.bin.packing.partitioner.http.client

import com.softwaremill.sttp._

import scala.util.Try

class HttpClient {

  private implicit val backend = HttpURLConnectionBackend()

  def head[T](url: String)(handler: Try[Id[Response[String]]] => T): T = {
    val request: Request[String, Nothing] = sttp.head(uri"$url")
    val response: Try[Id[Response[String]]] = Try(backend.send(request))
    handler(response)
  }

}
