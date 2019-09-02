package com.gelerion.spark.bin.packing.partitioner.utils

case class Trie[V](value: Option[V], children: List[Option[Trie[V]]], childrenAreEmpty: Boolean = true) {

  def insert(key: String, value: V): Trie[V] = Trie.insert(this, key, value, 0)

  def delete(key: String): Trie[V] = Trie.delete(this, key, 0)

  def search(key: String): Option[V] = Trie.search(this, key, 0)

  def startsWith(key: String): Boolean = Trie.startsWith(this, key, 0)
}

object Trie {
  def empty[V]: Trie[V] = new Trie[V](None, List.fill(91)(None))

  def apply[V]: Trie[V] = empty[V]

  private def search[V](node: Trie[V], key: String, step: Int): Option[V] =
    if (key.length == step) {
      node.value
    } else {
      node.children(key.charAt(step) - 32) match {
        case Some(nextItem) => search(nextItem, key, step + 1)
        case None           => None
      }
    }

  private def startsWith[V](node: Trie[V], key: String, step: Int): Boolean =
    if (key.length == step) {
      node.childrenAreEmpty
    } else {
      val idx = key.charAt(step) - 32
      if (idx < 0 || idx >= 91) return false //definitely not in a trie
      node.children(idx) match {
        case Some(nextItem) => startsWith(nextItem, key, step + 1)
        case None           => if (node.childrenAreEmpty) true else false
      }
    }

  private def insert[V](node: Trie[V], key: String, value: V, step: Int): Trie[V] =
    if (key.length == step) {
      node.copy(value = Some(value), childrenAreEmpty = true)
    } else {
//      val index0    = key.charAt(step) - 97
//      if (index0 < 0) println(s"Invalid idx $index0 for ${key.charAt(step)}")

      val index    = key.charAt(step) - 32
      val nextItem = node.children(index).getOrElse(Trie.empty[V])
      val newNode  = insert(nextItem, key, value, step + 1)
      val newNext  = node.children.updated(index, Some(newNode))

      node.copy(children = newNext, childrenAreEmpty = false)
    }

  private def delete[V](node: Trie[V], key: String, step: Int): Trie[V] =
    if (key.length == step) {
      node.copy(value = None)
    } else {
      val index = key.charAt(step) - 32
      node.children(index) match {
        case None           => node
        case Some(nextItem) =>
          val newNode     = delete(nextItem, key, step + 1)
          val newChildren =
            if (newNode.value.isEmpty && newNode.children.forall(_.isEmpty))
              node.children.updated(index, None)
            else
              node.children.updated(index, Some(newNode))

          node.copy(children = newChildren)
      }
    }
}

object test {
  def main(args: Array[String]): Unit = {
    //    val trie = Trie[Boolean]
    //      .insert("abc", true)
    //      .insert("abv", true)
    //      .insert("aev", true)
    //      .insert("aec", true)
    //      .insert("*** START OF THIS PROJECT GUTENBERG", true)


    val str = "Title: Prester John\n\nAuthor: John Buchan\n\nPosting Date: October 10, 2008 [EBook #611]\nRelease Date: August, 1996\n[Last updated: August 19, 2012]\n\nLanguage: English\n\nCharacter set encoding: ASCII\n\n*** START OF THIS PROJECT GUTENBERG EBOOK PRESTER JOHN ***\n\n\n\n\nProduced by Jo Churcher.  HTML version by Al Haines.\nPRESTER JOHN\n\n\nby\n\nJOHN BUCHAN\n\n\n\n\nTO\n\nLIONEL PHILLIPS\n*** END OF THIS PROJECT GUTENBERG EBOOK PRESTER JOHN ***\n\n***** This file should be named 611.txt or 611.zip *****\nThis and all associated files of various formats will be found in:\n        http://www.gutenberg.org/6/1/611/\n\nProduced by Jo Churcher.  HTML version by Al Haines."
    //    println(str.split("\n")
    //      .toStream
    //      .dropWhile(line => !GutenbergLibrary.textStartMarkers.startsWith(line))
    //      .tail
    //      .takeWhile(line => !GutenbergLibrary.textEndMarkers.startsWith(line))
    //      .mkString(""))


    //      .map(line => {
    //      if (trie.startsWith(line)) { println(s"True for $line"); true } else false


    //    println(trie.search("abcd"))
    //    println(trie.startsWith("aecsdd"))
  }
}