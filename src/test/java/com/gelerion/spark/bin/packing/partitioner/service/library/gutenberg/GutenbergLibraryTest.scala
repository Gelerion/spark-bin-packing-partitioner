package com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg

import com.gelerion.spark.bin.packing.partitioner.domain.model.{EBooksUrls, Ebook, EbookUrl}
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
//      println(gutenbergLibrary.resolveEbookUrls(bookshelf))
//    })
  }

  it should "successfully get text for a e-book" in {
    val urls = EBooksUrls("https://www.gutenberg.org/wiki/Adventure_(Bookshelf)",
      Seq(EbookUrl(Ebook(611, "Prester John"), "http://www.gutenberg.lib.md.us/6/1/611/611.txt", 428431)), 428431)

    val ebooks = gutenbergLibrary.getEbooksTexts(urls)
    assert(ebooks.size == 1)
    assert(!ebooks.head.text.isEmpty)
  }

  it should "not fail when ebook url is wrong or has no content" in {
    val urls = EBooksUrls("https://www.gutenberg.org/wiki/Adventure_(Bookshelf)",
      Seq(EbookUrl(Ebook(611, "Prester John"), "http://www.gutenberg.lib.md.us/wrong.txt", -1)), -1)
    val ebooks = gutenbergLibrary.getEbooksTexts(urls)
    assert(ebooks.isEmpty)
  }
}
