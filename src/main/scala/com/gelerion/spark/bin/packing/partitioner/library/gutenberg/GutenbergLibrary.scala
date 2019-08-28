package com.gelerion.spark.bin.packing.partitioner.library.gutenberg

import com.gelerion.spark.bin.packing.partitioner.domain.serde.BookshelfSerDe
import com.gelerion.spark.bin.packing.partitioner.domain._
import com.gelerion.spark.bin.packing.partitioner.service.html.parser.HtmlParser
import com.gelerion.spark.bin.packing.partitioner.library.gutenberg.GutenbergLibrary.{textEndMarkers, textStartMarkers}
import com.gelerion.spark.bin.packing.partitioner.service.WebClient
import com.gelerion.spark.bin.packing.partitioner.utils.Trie
import org.apache.logging.log4j.scala.Logging

import scala.io.{BufferedSource, Codec}
import scala.reflect.io.File

//http://www.gutenberg.lib.md.us
//Project Gutenberg offers over 59,000 free eBooks. Choose among free epub and Kindle eBooks, download them or
//read them online. You will find the world's great literature here, with focus on older works for which U.S.
//copyright has expired. Thousands of volunteers digitized and diligently proofread the eBooks, for enjoyment and education.
class GutenbergLibrary(htmlParser: HtmlParser = HtmlParser(),
                       webClient: WebClient = WebClient())
  extends EbooksLibrary
  with Logging {
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

  override def getBookshelvesWithEbooks: Seq[Bookshelf] = {
    logger.info("*** GETTING BOOK IDS")
    //get-bookshelf-ids-and-titles!
    getAllBookshelfUrls()
      .map(bookshelfUrl => Bookshelf(bookshelfUrl, getEbookIdsAndTitles(bookshelfUrl).toSeq))
      .toSeq
  }

  override def getEbooksTexts(ebooksUrls: EBooksUrls): Seq[EbookText] = {
    //get-ebook-texts
    ebooksUrls.booksUrls.map(bookUrl => {
      logger.debug(s"Getting text of ${bookUrl.ebook.id} - ${bookUrl.ebook.title}, ${bookUrl.url} ...")
      getEbookText(bookUrl)
    })
  }

  override def resolveEbookUrls(bookshelf: Bookshelf): EBooksUrls = {
    //bookshelf-ebooks
    logger.debug(s"Fetching ebook urls from ${bookshelf.url}")
    val ebookUrls = getEbookUrls(bookshelf)
    EBooksUrls(bookshelf.url, ebookUrls, ebookUrls.map(_.length).sum)
  }

  private def getEbookText(ebookUrl: EbookUrl): EbookText  = {
    val text = webClient.readText(ebookUrl)
      .dropWhile(line => !textStartMarkers.startsWith(line))
      .tail //drop start marker
      .takeWhile(line => !textEndMarkers.startsWith(line))
      .mkString("")

    EbookText(ebookUrl.ebook, text)
  }

  private def getEbookUrls(bookshelf: Bookshelf): Seq[EbookUrl] = {
    bookshelf.ebooks.flatMap(ebook => generateEbookUrl(ebook))
  }

  private def generateEbookUrl(ebook: Ebook): Option[EbookUrl] = {
    //generate-ebook-urls
    logger.trace(s"Generating url for $ebook")
    webClient.generateUrlFor(ebook)

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
//    new GutenbergLibrary().getBookshelvesWithEbooks().foreach(println)

    //create bookshelves
    val library = new GutenbergLibrary()
//    val bookshelves = library.getBookshelvesWithEbooks()
//    File("bookshelves").writeAll(BookshelfSerDe.encode(bookshelves))
    val requested = 1
    val source: BufferedSource = File("bookshelves").chars(Codec.UTF8)
    val bookshelves = source.getLines().take(requested).flatMap(BookshelfSerDe.decode).flatten


//    for (bookshelf <- bookshelves;
//         urls: EbookUrl <- library.getEbookUrls(bookshelf)) {
//
//    }

    val urls = bookshelves.map(library.resolveEbookUrls)
    urls.foreach(url => println(url))
    source.close()

//    val bookshelves = Seq(
//      Bookshelf("url1", Seq(Ebook(1, "title1"), Ebook(2, "title2"))),
//      Bookshelf("url2", Seq(Ebook(3, "title3"), Ebook(4, "title4")))
//    )
//
//    val encoded = BookshelfSerDe.encode(bookshelves)
//    println(encoded)
//
//    println(BookshelfSerDe.decode(encoded))
//    val a = bookshelves.map(b => s"${b.url}***${b.ebooks.map(e => s"${e.id}^^${e.title}").mkString(" ")}").mkString("\n")
//    println(a)

//    Files.write(Paths.get())
//    val strings: Seq[String] = bookshelves.map(_.toString)


//    library.getBookshelvesWithEbooks().take(2).foreach(shelf => {
//      library.getEbookUrls(shelf).foreach(println)
//    })
  }

}

object GutenbergLibrary {
  private val textStartMarkers: Trie[Boolean] = Set(
    "*END*THE SMALL PRINT",
    "*** START OF THE PROJECT GUTENBERG",
    "*** START OF THIS PROJECT GUTENBERG",
    "This etext was prepared by",
    "E-text prepared by",
    "Produced by",
    "Distributed Proofreading Team",
    "Proofreading Team at http://www.pgdp.net",
    "http://gallica.bnf.fr)",
    "      http://archive.org/details/",
    "http://www.pgdp.net",
    "by The Internet Archive)",
    "by The Internet Archive/Canadian Libraries",
    "by The Internet Archive/American Libraries",
    "public domain material from the Internet Archive",
    "Internet Archive)",
    "Internet Archive/Canadian Libraries",
    "Internet Archive/American Libraries",
    "material from the Google Print project",
    "*END THE SMALL PRINT",
    "***START OF THE PROJECT GUTENBERG",
    "This etext was produced by",
    "*** START OF THE COPYRIGHTED",
    "http://gutenberg.spiegel.de/ erreichbar.",
    "Project Runeberg publishes",
    "Beginning of this Project Gutenberg",
    "Project Gutenberg Online Distributed",
    "Gutenberg Online Distributed",
    "the Project Gutenberg Online Distributed",
    "Project Gutenberg TEI",
    "This eBook was prepared by",
    "http://gutenberg2000.de erreichbar.",
    "This Etext was prepared by",
    "This Project Gutenberg Etext was prepared by",
    "Gutenberg Distributed Proofreaders",
    "Project Gutenberg Distributed Proofreaders",
    "the Project Gutenberg Online Distributed Proofreading Team",
    "**The Project Gutenberg",
    "*SMALL PRINT!",
    "More information about this book is at the top of this file.",
    "tells you about restrictions in how the file may be used.",
//    "l'authorization à les utilizer pour preparer ce texte.",
    "l'authorization",
    "of the etext through OCR.",
    "*****These eBooks Were Prepared By Thousands of Volunteers!*****",
    "We need your donations more than ever!",
    " *** START OF THIS PROJECT GUTENBERG",
    "****     SMALL PRINT!",
    "[\"Small Print\" V.",
    "      (http://www.ibiblio.org/gutenberg/",
    "and the Project Gutenberg Online Distributed Proofreading Team",
    "Mary Meehan, and the Project Gutenberg Online Distributed Proofreading",
    "                this Project Gutenberg edition.")
    .aggregate(Trie.empty[Boolean])(
    seqop = (trie, key) => trie.insert(key, true),
    combop = (trie, _) => trie)

  val textEndMarkers: Trie[Boolean] = Set(
    "*** END OF THE PROJECT GUTENBERG",
    "*** END OF THIS PROJECT GUTENBERG",
    "***END OF THE PROJECT GUTENBERG",
    "End of the Project Gutenberg",
    "End of The Project Gutenberg",
    "Ende dieses Project Gutenberg",
    "by Project Gutenberg",
    "End of Project Gutenberg",
    "End of this Project Gutenberg",
    "Ende dieses Projekt Gutenberg",
    "        ***END OF THE PROJECT GUTENBERG",
    "*** END OF THE COPYRIGHTED",
    "End of this is COPYRIGHTED",
    "Ende dieses Etextes ",
    "Ende dieses Project Gutenber",
    "Ende diese Project Gutenberg",
    "**This is a COPYRIGHTED Project Gutenberg Etext, Details Above**",
    "Fin de Project Gutenberg",
    "The Project Gutenberg Etext of ",
    "Ce document fut presente en lecture",
    "Ce document fut pr",
    "More information about this book is at the top of this file.",
    "We need your donations more than ever!",
    "END OF PROJECT GUTENBERG",
    " End of the Project Gutenberg",
    " *** END OF THIS PROJECT GUTENBERG"
  ).aggregate(Trie.empty[Boolean])(
    seqop = (trie, key) => trie.insert(key, true),
    combop = (trie, _) => trie)

}