package com.gelerion.spark.bin.packing.partitioner.domain

import com.gelerion.spark.bin.packing.partitioner.utils.Rational

import scala.collection.immutable.ListMap

object tfidf

case class EBookTerms(ebookId: Int, terms: Seq[String])

case class EBookTermFreq(ebookId: Int, termFreqRelation: Seq[(String, Rational)])
case class EbookTfIdf(ebook: Ebook, tfidf: ListMap[String, Double])