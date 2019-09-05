package com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg

import org.scalatest.FlatSpec

class GutenbergLibraryTest extends FlatSpec {
  behavior of "GutenbergLibrary"

  private val gutenbergLibrary = new GutenbergLibrary()
  import gutenbergLibrary.implicits._

  it should "load bookshelves and propagate limits" in {
    val limitBookshelves = 2
    val limitEbooks = 3

    val bookshelves = gutenbergLibrary.getBookshelvesWithEbooks
      .limitEBooksPerShelf(limitEbooks)
      .limitBookshelves(limitBookshelves)

    assert(bookshelves.size == limitBookshelves)
    assert(bookshelves.flatMap(_.ebooks).size <= (limitEbooks * limitBookshelves)) //up to 3 ebooks per shelf

//    bookshelves.foreach(bookshelf => {
//      println(bookshelf.url)
//      println(s"ebooks count ${bookshelf.ebooks.size}")
//      bookshelf.ebooks.foreach(ebook => s" -> ${println(ebook)}")
//    })
  }
}
