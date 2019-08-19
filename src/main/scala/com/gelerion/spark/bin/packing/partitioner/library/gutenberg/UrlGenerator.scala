package com.gelerion.spark.bin.packing.partitioner.library.gutenberg

import com.gelerion.spark.bin.packing.partitioner.domain.{Ebook, EbookUrl}
import com.gelerion.spark.bin.packing.partitioner.http.client.HttpClient

import scala.util.{Failure, Success}

case class UrlGenerator(httpClient: HttpClient = new HttpClient) {
  private val root = "http://www.gutenberg.lib.md.us"
  private val exts = ".txt" :: "-8.txt" :: "-0.txt" :: Nil

  def generateFor(ebook: Ebook): Option[EbookUrl] = {
    val id = ebook.id
    //750702 -> 7/5/0/7/0
    val path = id.toString.dropRight(1).toCharArray.mkString("/")

    //when using Stream as a lazy list, we basically say:
    //start invoking the functions and give me the first that does not evaluate to empty.
    val maybeUrlsStream: Stream[Option[EbookUrl]] = Stream(exts:_ *)
      .map(ext => {
        val urlPath = s"$root/$path/$id/$id$ext"
        httpClient.head(urlPath) {
          case Success(resp) => resp.header("Content-Length").map(_.toInt).map(length => EbookUrl(ebook, urlPath, length))
          case Failure(exception) => None
        }
      })
      .dropWhile(_.isEmpty)

    //head of empty stream throws exception, so we have to validate if stream isn't empty
    maybeUrlsStream match {
      case Stream.Empty => None
      case head #:: tail => head
    }

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
    println(UrlGenerator().generateFor(Ebook(24583, "abc")))
  }
}