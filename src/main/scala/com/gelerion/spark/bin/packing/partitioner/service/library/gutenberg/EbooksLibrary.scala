package com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg

import com.gelerion.spark.bin.packing.partitioner.domain.model.{Bookshelf, EBooksUrls, EbookText}

trait EbooksLibrary {

  def getBookshelvesWithEbooks: Seq[Bookshelf]

  def resolveEbookUrls(bookshelf: Bookshelf): EBooksUrls

  def getEbooksTexts(ebooksUrls: EBooksUrls): Seq[EbookText]

}
