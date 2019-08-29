package com.gelerion.spark.bin.packing.partitioner.service

import com.gelerion.spark.bin.packing.partitioner.domain.model.{Ebook, EbookUrl}
import com.gelerion.spark.bin.packing.partitioner.http.client.HttpClient

import scala.util.{Failure, Success}

case class WebClient(httpClient: HttpClient = new HttpClient) {
  private val root = "http://www.gutenberg.lib.md.us"
  private val exts = ".txt" :: "-8.txt" :: "-0.txt" :: Nil

  def generateUrlFor(ebook: Ebook): Option[EbookUrl] = {
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

    //head on empty stream throws exception
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

  def readText(ebookUrl: EbookUrl): Stream[String] = {
    httpClient.get(ebookUrl.url).split(System.lineSeparator()).toStream
  }
}


object TestGen {
  def main(args: Array[String]): Unit = {
//    println(WebClient().generateUrlFor(Ebook(24583, "abc")))

//    WebClient().readText(EbookUrl(null, "http://www.gutenberg.lib.md.us/6/1/611/611.txt", 0))
//      .dropWhile(line => !GutenbergLibrary.textStartMarkers.startsWith(line))
//      .tail
//      .takeWhile(line => !GutenbergLibrary.textEndMarkers.startsWith(line))
//      .foreach(text => println(text))


//    val text = WebClient().readText(EbookUrl(null, "http://www.gutenberg.lib.md.us/6/1/611/611.txt", 0))
//        .dropWhile(row => {
//          val bool = GutenbergLibrary.textStartMarkers.contains(row)
//          if (!bool) println(s"row start marker $row")
//          bool
//        })
//    println(text.force)
  }
}