package com.gelerion.spark.bin.packing.partitioner.library.gutenberg

import com.gelerion.spark.bin.packing.partitioner.html.parser.HtmlParser
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import org.apache.logging.log4j.scala.Logging

//http://www.gutenberg.lib.md.us
//Project Gutenberg offers over 59,000 free eBooks. Choose among free epub and Kindle eBooks, download them or
//read them online. You will find the world's great literature here, with focus on older works for which U.S.
//copyright has expired. Thousands of volunteers digitized and diligently proofread the eBooks, for enjoyment and education.
class GutenbergLibrary(htmlParser: HtmlParser = HtmlParser(),
                       urlGenerator: UrlGenerator = UrlGenerator()) extends Logging{
  private val baseUrl = "https://www.gutenberg.org"
  private val bookshelfUrl1 = baseUrl + "/wiki/Category:Bookshelf"
  private val bookshelfUrl2 = baseUrl + "/w/index.php?title=Category:Bookshelf&pagefrom=The+Girls+Own+Paper+%28Bookshelf%29#mw-pages"

  private val nonBookshelfHrefs = Set(
    "/wiki/Category:DE_B%C3%BCcherregal",
    "/wiki/Category:FR_Genre",
    "/wiki/Category:IT_Biblioteca",
    "/wiki/Category:PT_Prateleira",
    "/wiki/Category:Categories",
    "/wiki/Category:Bookshelf",
    "/wiki/Special:Categories",
    "/wiki/Special:Search",
    "/wiki/Main_Page")

  private val bookshelfHrefRegex = "^/wiki/.+\\(Bookshelf\\)"

  def getEbookUrls(bookshelf: Bookshelf) = {
    //bookshelf-ebooks
    bookshelf.ebooks
      .map(ebook => generateEbookUrl(ebook))
  }

  def generateEbookUrl(ebook: Ebook) = {
    //generate-ebook-urls
    urlGenerator.generateFor(ebook)

  }

  def getBookshelvesWithEbooks(): Seq[Bookshelf] = {
    logger.info("*** GETTING BOOK IDS")
    //get-bookshelf-ids-and-titles!
    getAllBookshelfUrls()
      .map(bookshelfUrl => Bookshelf(bookshelfUrl, getEbookIdsAndTitles(bookshelfUrl).toSeq))
      .toSeq
  }

  private def getEbookIdsAndTitles(bookshelfUrl: String): Iterable[Ebook] = {
    //get-bookshelf-ebook-ids-and-titles
    //get-ebook-ids-and-titles
    htmlParser.browse(bookshelfUrl) { doc =>
      import net.ruippeixotog.scalascraper.dsl.DSL._

      for {
        hyperlink <- doc >> "a" if hyperlink.hasAttr("title")
        title = hyperlink.attr("title") if title.startsWith("ebook:") && title.length > "ebook:".length + 1
        id = title.substring("ebook:".length).toInt
      } yield Ebook(id, hyperlink.text)
    }
  }

  private def getAllBookshelfUrls(): Iterable[String] = {
    //get-all-bookshelf-urls!
    getBookShelves(getHrefs(bookshelfUrl1) ++ getHrefs(bookshelfUrl2))
      .map(bookShelfHref => baseUrl + bookShelfHref)
  }

  private def getBookShelves(hrefs: Iterable[String]): Iterable[String] = {
    //get-bookshelves [hrefs]
    hrefs
      .filterNot(href => href == null)
      .filterNot(href => nonBookshelfHrefs.contains(href))
      .filter(href => href matches bookshelfHrefRegex)
  }

  private def getHrefs(bookshelfUrl: String): Iterable[String] = {
    //get-attr url-path :href
    htmlParser.browse(bookshelfUrl) { doc =>
      import net.ruippeixotog.scalascraper.dsl.DSL._

      for {
        hyperlink <- doc >> "a" if hyperlink.hasAttr("href")
        href = hyperlink.attr("href")
      } yield href
    }
  }
}

object Test {
  def main(args: Array[String]): Unit = {
//    new GutenbergLibrary().getAllBookshelfUrls().foreach(println)
//    new GutenbergLibrary().getEbookIdsAndTitles("https://www.gutenberg.org/wiki/Zoology_(Bookshelf)").foreach(println)
    new GutenbergLibrary().getBookshelvesWithEbooks().foreach(println)
  }

}