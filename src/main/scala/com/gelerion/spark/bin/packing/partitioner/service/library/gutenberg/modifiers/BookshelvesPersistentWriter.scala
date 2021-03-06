package com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg.modifiers

import com.gelerion.spark.bin.packing.partitioner.domain.model.Bookshelf
import com.gelerion.spark.bin.packing.partitioner.domain.model.serde.BookshelfSerDe
import com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg.EbooksLibrary

import scala.reflect.io.File
import scala.util.Try

/**
 * Note, when used doesn't honor laziness and always evaluates eagerly.
 */
trait BookshelvesPersistentWriter extends EbooksLibrary {
  private val filename = "bookshelves"
  private lazy val file = File(filename)

  abstract override def getBookshelvesWithEbooks: Seq[Bookshelf] = {
    val bookshelves = super.getBookshelvesWithEbooks //parse web

    if (file.exists) {
      bookshelves
    } else {
      saveToFile(bookshelves)
    }
  }

  private def saveToFile(bookshelves: Seq[Bookshelf]): Seq[Bookshelf] = {
    Try(File("bookshelves").writeAll(BookshelfSerDe.encode(bookshelves)))
    bookshelves
  }
}
