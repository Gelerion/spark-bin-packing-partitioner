package com.gelerion.spark.bin.packing.partitioner.service

import com.gelerion.spark.bin.packing.partitioner.domain.{model, _}
import com.gelerion.spark.bin.packing.partitioner.domain.model.{BookshelfText, BookshelfUrl, Ebook, EbookText, EbookTfIdf}
import com.gelerion.spark.bin.packing.partitioner.service.TfIdf.getTerms
import com.gelerion.spark.bin.packing.partitioner.utils.Rational
import org.apache.logging.log4j.scala.Logging

import scala.collection.immutable.{ListMap, SortedMap}
import scala.language.implicitConversions

/**
 * https://www.onely.com/blog/what-is-tf-idf/
 *
 * Gather words. Write your content. Run a TF*IDF report for your words and get their weights.
 * The higher the numerical weight value, the rarer the term.
 * The smaller the weight, the more common the term.
 */
case class TfIdf[DocId](corpus: Map[DocId, String]) extends Logging {

  def calculate(): Map[DocId, TermsWeightsMap] = {
    val (tfs, idf) = calcTfAndIdf

    //idf has all the words across documents
    tfs.mapValues(tf => {
      tf.map { case (term, freqOfTermInDoc) => (term, calcTermWeight(freqOfTermInDoc, idf.getOrElse(term, 0D))) }
    })
      //Map[DocId, Map[Term, Weight]]
      .mapValues(termWeights => {
        //sort by term weigh desc, take the most significant only
        val mostSignificantTerms = termWeights.toSeq.sortWith(_._2 > _._2).take(500)
        TermsWeightsMap(ListMap(mostSignificantTerms: _*))
      })
  }

  /**
   * tf - number of times term appears in a document
   * idf - the measure of how significant that term is in the whole corpus
   * Weight = (TF * IDF)
   */
  private def calcTermWeight(freq: Rational, idf: Double): Double = {
    freq.value * idf
  }

  private def calcTfAndIdf: (Map[DocId, Map[String, Rational]], Map[String, Double]) = {
    //calc-tf-and-idf [id-doc-pairs]

    val idAndTerms = corpus.mapValues(getTerms)
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
  private def idf(nDocs: Long, termDocCounts: Map[String, Int]): Map[String, Double] = {
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
  private def tf(terms: Stream[String]): Map[String, Rational] = {
    val nTerms = terms.length
    val calcFreqRelation = calcTf(nTerms)_

    logger.debug(s"Calculating tf for $nTerms terms ...")
    val frequencies = terms.groupBy(identity).mapValues(_.length)
    frequencies.mapValues(calcFreqRelation)
  }

  private def calcTf(nTerms: Int)(termFreq: Int): Rational = {
    Rational(termFreq, nTerms)
  }
}

object TfIdf {
  private val stopWord: Set[String] = Set("","a","all","and","any","are","is","in","of","on",
    "or","our","so","this","the","that","to","we","it","for")

  implicit def ebookTextToMap(corpus: Seq[EbookText]): Map[Ebook, String] = corpus.map(ebook => (ebook.id, ebook.text)).toMap
  implicit def tfidfToEbookTfIdf(tfidf: Map[Ebook, TermsWeightsMap]): Seq[EbookTfIdf] = tfidf.map {
    case (ebook, weights) => model.EbookTfIdf(ebook, weights)
  }.toSeq

  def calculate(corpus: Seq[EbookText]): Seq[EbookTfIdf] = {
    new TfIdf(corpus).calculate()
  }

  def calculate(url: BookshelfUrl, bookshelf: BookshelfText): Map[String, TermsWeightsMap] = {
    new TfIdf(Map(url.value -> bookshelf.text)).calculate()
  }

  def calculate(docs: Map[String, String]): TfIdfIndex = TfIdfIndex(TfIdf(docs).calculate())

  def getTerms(text: String): Stream[String] = {
    //improvement: build lemmas
    //separate class
    splitWord(text).filterNot(stopWord).map(cleanPunctuation).filter(notEmptyOrSmallWord)
  }

  def tf(docs: Map[String, String]): Map[String, TermsWeightsMap] = {
    val (tfs, _) = TfIdf(docs).calcTfAndIdf
    tfs.mapValues(docTf => TermsWeightsMap(ListMap(docTf.mapValues(_.value).toSeq.sortWith(_._2 > _._2):_ *)))
  }

  def idf(docs: Map[String, String]): Map[String, Double] = {
    val (_, idf) = TfIdf(docs).calcTfAndIdf
    idf
  }

  private def splitWord(text: String): Stream[String] = {
    text.toLowerCase.split("\\W+").toStream
  }

  private def cleanPunctuation(word: String): String = {
    word.trim.replaceAll("(\\p{Punct})", "")
  }

  private def notEmptyOrSmallWord(word: String): Boolean = {
    word.nonEmpty && word.length > 1
  }
}

case class TfIdfIndex(private val tfIdfPerDoc: Map[String, TermsWeightsMap]) {
  type Term = String
  type DocId = String
  //private lazy val invertedTermsIndex: Map[Term, DocId] = tfIdfPerDoc

  def getSignatureWordsFor(docId: DocId): Iterator[TermWeight] = {
    tfIdfPerDoc(docId).iterator
  }
}

//term to weight, values are always sorted by weight ascending
case class TermsWeightsMap(private val dict: ListMap[String, Double]) extends Iterable[TermWeight]  {
  type Term = String
  type Weight = Double

  def getTermWeight(term: Term): Weight = dict.getOrElse(term, 0D)

  def terms: Iterator[TermWeight] = iterator

  override def iterator: Iterator[TermWeight] = dict.map { case (term, weight) => TermWeight(term, weight) }.iterator

}
case class TermWeight(term: String, weight: Double) extends Ordered[TermWeight] {
  override def compare(that: TermWeight): Int = (this.weight - that.weight).toInt
}

object testTfIdf {

  def main(args: Array[String]): Unit = {
    import TfIdf.ebookTextToMap
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

    val tfIdf: TfIdf[Ebook] = TfIdf(corpus)
    println(tfIdf.calculate())

    //println(tfIdf.calc())
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