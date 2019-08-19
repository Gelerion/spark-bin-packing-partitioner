package com.gelerion.spark.bin.packing.partitioner.domain

object library
case class Ebook(id: Int, title: String)
case class Bookshelf(url: String, ebooks: Seq[Ebook])
case class EbookUrl(ebook: Ebook, url: String, length: Int)
case class BookshelfEBooksSummary(bookshelf: Bookshelf, books: Seq[EbookUrl], totalSize: Int)
