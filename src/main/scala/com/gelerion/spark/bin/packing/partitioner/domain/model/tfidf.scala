package com.gelerion.spark.bin.packing.partitioner.domain.model

import com.gelerion.spark.bin.packing.partitioner.service.TermsWeightsMap
import com.gelerion.spark.bin.packing.partitioner.utils.Rational

object tfidf

case class EBookTerms(ebookId: Int, terms: Seq[String])

case class EBookTermFreq(ebookId: Int, termFreqRelation: Seq[(String, Rational)])
//case class EbookTfIdf(ebook: Ebook, tfidf: ListMap[String, Double])
case class EbookTfIdf(ebook: Ebook, tfidf: TermsWeightsMap)