package com.gelerion.spark.bin.packing.partitioner.service.html.parser

import net.ruippeixotog.scalascraper.browser.{Browser, JsoupBrowser}

case class HtmlParser(private val browser: Browser = JsoupBrowser()) {

  def browse[T](url: String)(parse: browser.DocumentType => T): T = {
    val documentType: browser.DocumentType = browser.get(url)
    parse(documentType)
  }

}
