package com.gelerion.spark.bin.packing.partitioner.domain.serde

import com.gelerion.spark.bin.packing.partitioner.domain.{Bookshelf, Ebook}

import scala.util.Try

trait StringSerDe[T] {
  def encode(item: T): String
  def decode(str: String): Option[T]
}

object BookshelfSerDe extends StringSerDe[Seq[Bookshelf]] {
  private val urlSeparator: String = "%%%"
  private val ebookSeparator: String = "##"
  private val idTitleSeparator: String = "!!!"

  override def encode(bookshelves: Seq[Bookshelf]): String = {
    bookshelves.map(bookshelf =>
      s"${bookshelf.url}$urlSeparator${bookshelf.ebooks.map(e => s"${e.id}$idTitleSeparator${e.title}").mkString(ebookSeparator)}").mkString("\n")
  }

  override def decode(str: String): Option[Seq[Bookshelf]] = {
    Try {
      str.split("\n")
        .map(row => {
          val url = row.split(urlSeparator)(0)
          val ebooks = row.split(urlSeparator)(1).split(ebookSeparator)
            .map(ebook => Ebook(ebook.split(idTitleSeparator)(0).toInt, ebook.split(idTitleSeparator)(1)))
          Bookshelf(url, ebooks)
        })
        .toSeq
    }.toOption
  }
}