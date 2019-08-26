package com.gelerion.spark.bin.packing.partitioner.service

import com.gelerion.spark.bin.packing.partitioner.domain.{EBookTermFreq, EBookTerms, Ebook, EbookText}
import com.gelerion.spark.bin.packing.partitioner.utils.Rational
import org.apache.logging.log4j.scala.Logging

import scala.collection.immutable

/**
 * https://www.onely.com/blog/what-is-tf-idf/
 *
 * Gather words. Write your content. Run a TF*IDF report for your words and get their weights.
 * The higher the numerical weight value, the rarer the term.
 * The smaller the weight, the more common the term.
 */
case class TfIdf(corpus: Seq[EbookText]) extends Logging {
  
  private val stopWord: Set[String] = Set("","a","all","and","any","are","is","in","of","on",
  "or","our","so","this","the","that","to","we","it","for")

  def calc() = {
    val (tfs, idf) = calcTfAndIdf
    //idf has all the words across documents

    //map-vals
    val stub: Map[Ebook, immutable.Iterable[Double]] = tfs.mapValues(tf => {
      tf.map { case (term, freq) => freq.value * idf.getOrElse(term, -1D)}
    })

    stub
//    stub.o
//    tfs.values
//    idf.values
//    idf.filterKeys(tfs.keys)


  }

  def calcTfAndIdf: (Map[Ebook, Map[String, Rational]], Map[String, Double]) = {
    //calc-tf-and-idf [id-doc-pairs]
    val idAndTerms = corpus.map(ebook => (ebook.id, getTerms(ebook.text))).toMap
    val tfs = idAndTerms.map {case (id, terms) => (id, tf(terms))}

    val docsCount = corpus.size
    val termDocCounts = idAndTerms
      //exclude duplicates within each text then concatenate
      .flatMap { case (_, terms) => terms.distinct }
      //group by words and count frequencies
      .groupBy(identity)
      .mapValues(_.size)

    (tfs, idf(docsCount, termDocCounts))
  }

  /**
   * The IDF (inverse document frequency) of a word is the measure of how significant that term is in the whole corpus.
   *
   * For example, say the term “cat” appears x amount of times in a 10,000,000 million document-sized corpus (i.e. web).
   * Let’s assume there are 0.3 million documents that contain the term “cat”, then the IDF (i.e. log {DF}) is given
   * by the total number of documents (10,000,000) divided by the number of documents containing the term “cat” (300,000)
   */
  def idf(nDocs: Long, termDocCounts: Map[String, Int]): Map[String, Double] = {
    logger.info(s"Calculating idf for $nDocs docs ...")
    termDocCounts.mapValues(termCount => Math.log(nDocs / (1.0 + termCount)))
  }

  //[#tuple[bookshelf-url {:ebooks [[ebook-id text]] :size total-ebook-size}]]
  /**
   * Frequency of term in doc
   *
   * For example, when a 100 word document contains the term “cat” 12 times, the TF for the word ‘cat’ is
   *   TFcat = 12/100 i.e. 0.12
   */
  def tf(terms: Stream[String]): Map[String, Rational] = {
    val nTerms = terms.length
    val calcFreqRelation = calcTf(nTerms)_

    logger.debug(s"Calculating tf for $nTerms terms ...")
    val frequencies = terms.groupBy(identity).mapValues(_.length)
    frequencies.mapValues(calcFreqRelation)
  }

  def calcTf(nTerms: Int)(termFreq: Int): Rational = {
    Rational(termFreq, nTerms)
  }
  
  def getTerms(text: String): Stream[String] = {
    splitWord(text).filterNot(stopWord)
  }
  
  def splitWord(text: String): Stream[String] = {
    text.toLowerCase.split("\\W+").toStream
  }
}

object TfIdf {
  def getTerms(ebooks: Seq[EbookText]): Seq[EBookTerms] = {
    val tfIdf = new TfIdf(ebooks)
    ebooks.map(ebook => EBookTerms(ebook.id.id, tfIdf.getTerms(ebook.text)))
  }

//  def tf(ebooks: Seq[EBookTerms]): Seq[EBookTermFreq] = {
//    val tfIdf = new TfIdf(ebooks)
//    ebooks.map(ebook => EBookTermFreq(ebook.ebookId, tfIdf.tf(ebook.terms).toSeq))
//  }
}


object testTfIdf {

  def main(args: Array[String]): Unit = {
    /*
    tf-idf [id-doc-pairs] / ebooks-tf-idf #(-> % :ebooks tf-idf/tf-idf) bs-texts
    (#sparkling/tuple["https://www.gutenberg.org/wiki/Adventure_(Bookshelf)"
                  {:ebooks ([[611 "Prester John"]
                             " Text "]
                            [[558 "The Thirty-nine Steps"]
                             " Text2 "])
                   :size 670799}])
     */

    val corpus = Seq(
      EbookText(Ebook(611, "Prester John"), " text abc dfe "),
      EbookText(Ebook(558, "The Thirty-nine Steps"), " text ddd "),
      EbookText(Ebook(453, "Pepper Pay"), " ddd qwe ref "),
      EbookText(Ebook(123, "Dauni"), "yh ui op")
    )

    val tfIdf: TfIdf = TfIdf(corpus)
    println(tfIdf.calcTfAndIdf)

    println(tfIdf.calc())
    val str = "At any rate it will not be my fault if they don't \"blossom come as the\nrose\".  Come out and visit rate us soon, man, and see the work you come had a\nhand in starting....'"
//    val str = "At any rate it will not be my fault if they don't \"blossom as the\nrose\".  Come out and visit us soon, man, and see the work you had a\nhand in starting....'"

//    tfIdf
//      .splitWord(str)
//      .foreach(println)


//    val terms = tfIdf.getTerms(str)
    //    terms.foreach(println)
//    val tfs = tfIdf.tf(terms)
    //    println(tfIdf.tf(terms))

    //(su/mapcat (fn [[k v]] (distinct v)) id-and-terms)
//    terms.distinct
//    val docCounts = 2;
  }

}