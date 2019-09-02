package com.gelerion.spark.bin.packing.partitioner.utils

import org.rogach.scallop.{ScallopConf, ScallopOption}

//noinspection TypeAnnotation
class CLI(arguments: Seq[String]) extends ScallopConf(arguments) {

  val isInLocalMode = opt[Boolean](
    name = "is-local-mode",
    descr = "Whether Spark be should run locally or not",
    default = Some(true)
  )

  val partMode = choice(
    name = "part-mode",
    choices = Seq("skew", "pack", "partition"),
    default = Some("skew")
  )

  val limitBookshelves = opt[Int](
    name = "limit-bookshelves",
    descr = "How many bookshelves to load",
    default = None
  )

  val limitEbooksPerBookshelf = opt[Int](
    name = "limit-ebooks-per-bookshelf",
    descr = "How many ebooks to load per bookshelf",
    default = None
  )

  val outputDirectory = opt[String](
    name = "output-directory",
    descr = "Where to store our new shiny index?",
    default = Some("tfidfIndex")
  )

  val searchQuery = opt[String](
    name = "search-query",
    descr = "Search for relevant ebooks",
    default = Some("how to cook dinner for forty large human people")
  )

  verify()
}

object Args {
  var cli: CLI = _

  def set(cli: CLI): Args.type = {
    this.cli = cli
    this
  }


}
