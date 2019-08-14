package com.gelerion.spark.bin.packing.partitioner.library.gutenberg

object books
case class Ebook(id: Int, title: String)
case class Bookshelf(url: String, ebooks: Seq[Ebook])
case class EbookUrl(url: String, length: Int)
