package com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg

import com.gelerion.spark.bin.packing.partitioner.domain.model.Bookshelf
import com.gelerion.spark.bin.packing.partitioner.service.BinPacking
import com.gelerion.spark.bin.packing.partitioner.utils.Args
import org.scalatest.FlatSpec

class GutenbergLibraryTest extends FlatSpec {
  behavior of "GutenbergLibrary"

  private val gutenbergLibrary = new GutenbergLibrary()

  it should "load bookshelves" in {
    val bookshelves = gutenbergLibrary.getBookshelvesWithEbooks
    val limited = withLimitsPushDown(bookshelves)(reqBookshelves = 3, reqEbook = 5)
    limited.foreach(println)
  }

  private def withLimitsPushDown(bookshelves: Seq[Bookshelf])(reqBookshelves: Int, reqEbook: Int): Seq[Bookshelf] = {
      bookshelves.take(reqBookshelves).map(bookshelf => bookshelf.copy(ebooks = bookshelf.ebooks.take(reqEbook)))
  }

}
