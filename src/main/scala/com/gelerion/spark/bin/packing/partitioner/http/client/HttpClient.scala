package com.gelerion.spark.bin.packing.partitioner.http.client

import com.softwaremill.sttp._

class HttpClient {

  private implicit val backend = HttpURLConnectionBackend()

  def head[T](url: String)(handler: Id[Response[String]] => T): T = {
    val request: Request[String, Nothing] = sttp.head(uri"$url")
    val response: Id[Response[String]] = backend.send(request)
    handler(response)
  }

}
