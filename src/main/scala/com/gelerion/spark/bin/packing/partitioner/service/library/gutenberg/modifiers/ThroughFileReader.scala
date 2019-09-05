package com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg.modifiers

import com.gelerion.spark.bin.packing.partitioner.domain.model.Bookshelf
import com.gelerion.spark.bin.packing.partitioner.domain.model.serde.BookshelfSerDe
import com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg.EbooksLibrary

import scala.io.Codec
import scala.language.reflectiveCalls
import scala.reflect.io.File

/**
 * stackable modifications
 * --
 * Make subsequent runs faster
 */
trait ThroughFileReader extends EbooksLibrary {
  implicit val codec: Codec = Codec.UTF8

  private val filename = "bookshelves"
  private lazy val file = File(filename)


  abstract override def getBookshelvesWithEbooks: Seq[Bookshelf] = {
    if (file.exists) {
      loadFromFile()
    } else {
      super.getBookshelvesWithEbooks
    }
  }

  private def loadFromFile(): Seq[Bookshelf] = {
    using(file.chars(codec)) { in =>
      in.getLines().flatMap(BookshelfSerDe.decode).flatten.toStream.force
    }
  }

  def using[A, B <: {def close(): Unit}] (closeable: B) (f: B => A): A =
    try { f(closeable) } finally { closeable.close() }
}
