package com.gelerion.spark.bin.packing.partitioner.library.gutenberg

import com.gelerion.spark.bin.packing.partitioner.http.client.HttpClient

case class UrlGenerator(httpClient: HttpClient = new HttpClient) {
  private val root = "http://www.gutenberg.lib.md.us"
  private val exts = ".txt" :: "-8.txt" :: "-0.txt" :: Nil

  def generateFor(ebook: Ebook) = {
    val id = ebook.id
    //750702 -> 7/5/0/7/0
    val path = id.toString.dropRight(1).toCharArray.mkString("/")

    //use Stream as a lazy list, and basically, you say:
    //start invoking the functions and give me the first that does not evaluate to empty.
    Stream(exts:_ *)
      .map(ext => {
        val urlPath = s"$root/$path/$id/$id$ext"
        httpClient.head(urlPath) { resp =>
          resp.header("Content-Length").map(_.toInt).map(length => EbookUrl(urlPath, length))
        }
      })
      .dropWhile(_.isEmpty).head

//    for (ext <- exts) {
//      val urlPath = s"$root/$path/$id/$id/$ext"
//      httpClient.head(urlPath) { resp =>
//        resp.header("Content-Length").map(_.toInt).get
//      }
//    }
  }
}


object TestGen {
  def main(args: Array[String]): Unit = {
    println(new UrlGenerator().generateFor(Ebook(24583, "abc")))
  }
}