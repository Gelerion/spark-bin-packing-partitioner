package com.gelerion.spark.bin.packing.partitioner.service

import org.scalatest._

class TfIdfTest extends FlatSpec with Matchers {
  behavior of "TfIdf"

  private val docs = Map(
    "doc 1" -> "word1 word1 word2 word3 word4",
    "doc 2" -> "word1 word2 word3 word4 word5",
    "doc 3" -> "word1 word1 word1 word1 word2",
  )

  it should "calculate terms frequencies per document" in {
    val docTfs = TfIdf.tf(docs)

    //doc 1 -> (word1, 0.4), (word2, 0.2), (word4, 0.2), (word3, 0.2)
    //doc 2 -> (word2, 0.2), (word5, 0.2), (word3, 0.2), (word1, 0.2), (word4,0.2)
    //doc 3 -> (word1, 0.8), (word2, 0.2)

    assert(docTfs("doc 1").getTermWeight("word1").equals(0.4))
    assert(docTfs("doc 2").getTermWeight("word1").equals(0.2))
    assert(docTfs("doc 3").getTermWeight("word1").equals(0.8))
  }

  it should "calculate IDF of a word where idf is the measure of how significant that term is in the whole corpus" in {
    val idfIndex = TfIdf.idf(docs)
    //`word 5` must be the most significant one as it is present only in `doc 2`
    val (mostSignificantTerm, weight) = idfIndex.iterator.maxBy{ case (term, weight) =>  weight}
    assert(mostSignificantTerm == "word5")
  }

  it should "calculate Tf-Idf aka signature words of the documents" in {
    val tfIdf = TfIdf.calculate(docs)
    val termWeightsIter = tfIdf.getSignatureWordsFor("doc 2").take(1)

    assert(termWeightsIter.next().term == "word5")
  }
}
