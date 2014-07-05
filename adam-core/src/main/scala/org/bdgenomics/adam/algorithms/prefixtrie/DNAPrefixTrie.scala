/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.algorithms.prefixtrie

import scala.collection.TraversableOnce

object DNAPrefixTrie {

  /**
   * Creates a fixed depth prefix trie, and populates it with a set of
   * key->value mappings. Keys containing ambiguous bases are not added to the
   * collection.
   *
   * @param init A map specifying key->value mappings. Used for populating the
   * initial trie.
   * @tparam V The type of the values stored in the trie.
   * @return Returns a populated trie.
   *
   * @throws AssertionError An assertion error is thrown if the length of the
   * keys provided varies or if the input is an empty map.
   */
  def apply[V](init: Map[String, V]): DNAPrefixTrie[V] = {
    assert(init.size > 0, "Cannot build empty prefix trie.")

    // keys must all be the same length
    val len = init.head._1.length
    assert(init.forall(kv => kv._1.length == len))

    // if key length is greater than 1, start nesting
    if (len > 1) {
      new NestedFixedDepthDNAPrefixTrie(init)
    } else {
      new FixedDepthDNAPrefixTrieValues(init)
    }
  }
}

/**
 * This trait represents a prefix trie that stores a mapping between string keys representing
 * DNA sequences, and values of any type. Classes that implement this trait are assumed to be
 * immutable. For DNA sequences, we allow the storage of unambiguous bases ("ACTG"). Search
 * methods accept the unambiguous bases and wildcards ("N" or "*").
 *
 * @tparam V The type of the values stored in the trie.
 */
trait DNAPrefixTrie[V] extends Serializable {

  /**
   * Checks whether this tree contains a specific key. The contains method accepts wildcards
   * (either '*' or 'N') for doing a wildcard content check.
   *
   * @param key Key to check for the existance of.
   * @return Returns true if the key is found.
   */
  def contains(key: String): Boolean

  /**
   * Gets the value of a key that is contained in the tree. This method throws an exception
   * if the key is not found in the tree. This method and all other getters do not take wildcards.
   *
   * @param key Key to search for the value of.
   * @tparam V Type of the values in this collection.
   * @return Returns the value corresponding to this key.
   *
   * @throws IllegalArgumentException Throws an exception if the key is not in the collection.
   *
   * @see getOrElse
   * @see getIfExists
   * @see search
   */
  def get(key: String): V

  /**
   * Gets the value of a key that is contained in the tree. If this key is not found, a user-provided
   * default value is returned. This method and all other getters do not take wildcards.
   *
   * @param key Key to search for the value of.
   * @param default Default value to return if the key is not found.
   * @tparam V Type of the values in this collection.
   * @return Returns the value corresponding to this key, or a default value if the key is not found.
   *
   * @see get
   * @see getIfExists
   * @see search
   */
  def getOrElse(key: String, default: V): V

  /**
   * Gets the value of a key that is contained in the tree, wrapped as an option. None is returned
   * if the key is not found. This method and all other getters do not take wildcards.
   *
   * @param key Key to search for the value of.
   * @tparam V Type of the values in this collection.
   * @return Returns the value wrapped in an option. None is returned if the key is not found.
   *
   * @see get
   * @see getOrElse
   * @see search
   */
  def getIfExists(key: String): Option[V]

  /**
   * Searches for all possible values that match a key, including wildcards. Returns the values
   * and keys that match the wildcard in a map.
   *
   * @param key Key to search for, with wildcards.
   * @tparam V Type of the values in this collection.
   * @return Returns a map containing all keys that match this search key, and their values.
   *
   * @see prefixSearch
   * @see suffixSearch
   */
  def search(key: String): Map[String, V]

  /**
   * Searches for all keys that match a given prefix. The prefix can include wildcards. The
   * behavior of this method is identical to the other search method.
   *
   * @param key Key to search for, with wildcards.
   * @tparam V Type of the values in this collection.
   * @return Returns a map containing all keys that match this search key, and their values.
   *
   * @see search
   * @see suffixSearch
   */
  def prefixSearch(key: String): Map[String, V]

  /**
   * Searches for all keys that match a given suffix. The suffix can include wildcards. The
   * behavior of this method is identical to the other search method.
   *
   * @param key Key to search for, with wildcards.
   * @tparam V Type of the values in this collection.
   * @return Returns a map containing all keys that match this search key, and their values.
   *
   * @see search
   * @see prefixSearch
   */
  def suffixSearch(key: String): Map[String, V]

  /**
   * Find provides identical functionality to search, except does not return a map. This
   * enables an optimization for recursive search, where the iterator is converted back to a map
   * at the last stage of the search. We do not expose this to users, since this method just
   * enables a performance optimization inside of the implementation.
   *
   * @note This method does not check for correct key length before being called, as it is a
   * package private method that should only be called after key length error checking.
   *
   * @param key Key to search for, with wildcards.
   * @tparam V Type of the values in this collection.
   * @return Returns a traversable collection containing tuples of all keys that match this search
   * key, and their values.
   */
  private[prefixtrie] def find(key: String): TraversableOnce[(String, V)]

  /**
   * Returns the number of key/value pairs stored in this trie.
   *
   * @return The count of key/value pairs in this trie.
   */
  def size: Int
}

/**
 * This class implements the non-terminal layers of our fixed-depth prefix trie, through a
 * recursive structure. Scaladoc is only included for the methods unique to this class,
 * otherwise see docs for the DNAPrefixTrie trait.
 *
 * @tparam V Type of the values stored by this class.
 *
 * @see DNAPrefixTrie
 */
private[prefixtrie] class NestedFixedDepthDNAPrefixTrie[V](init: Map[String, V]) extends DNAPrefixTrie[V] {

  // the length of keys stored at this level of the nested tree
  val len = init.head._1.length

  // fixed length prefix map
  val prefixMap: Array[Option[DNAPrefixTrie[V]]] = new Array(4)

  // create tree
  val tmp = init.toArray.map(kv => (cToI(kv._1.head), (kv._1.drop(1), kv._2)))

  // loop over all possible entry values
  (0 to 3).foreach(i => {
    // collect all values that map here
    val iVals = tmp.filter(t => t._1 == i).map(t => t._2).toMap

    // if we have elements in 
    prefixMap(i) = if (iVals.size > 0) {
      Some(DNAPrefixTrie(iVals))
    } else {
      None
    }
  })

  /**
   * Converts internal character codes into array indices.
   *
   * @param c Character to map to indices.
   * @return Array indices.
   */
  protected def cToI(c: Char): Int = c match {
    case 'A'       => 0
    case 'C'       => 1
    case 'G'       => 2
    case 'T'       => 3
    case '*' | 'N' => -1
    case _         => throw new IllegalArgumentException("Element " + c + " not an allowable character.")
  }

  /**
   * Converts internal array indices back to character codes.
   *
   * @param i Index to map.
   * @return Character code for this index.
   */
  protected def iToC(i: Int): Char = i match {
    case 0 => 'A'
    case 1 => 'C'
    case 2 => 'G'
    case 3 => 'T'
    case _ => throw new IllegalArgumentException("Index " + i + " not an allowable index.")
  }

  def contains(key: String): Boolean = {
    assert(key.length == len,
      "Provided key has incorrect length: " + key.length + ", expected " + len)

    // get the mapping bucket
    val bucket = cToI(key.head)
    val next = key.drop(1)

    // do we need to search all buckets? if we do, search, else go to the bucket
    if (bucket == -1) {
      prefixMap.flatMap(v => v).map(_.contains(next)).fold(false)(_ || _)
    } else {
      prefixMap(bucket).fold(false)(_.contains(next))
    }
  }

  def getIfExists(key: String): Option[V] = {
    assert(key.length == len,
      "Provided key has incorrect length: " + key.length + ", expected " + len)
    assert(key.head != '*',
      "Cannot perform wildcard search for value (key = " + key + ").")

    // get the mapping bucket
    val bucket = cToI(key.head)
    val next = key.drop(1)

    // map bucket
    prefixMap(bucket).fold(None.asInstanceOf[Option[V]])(_.getIfExists(next))
  }

  def getOrElse(key: String, default: V): V = {
    assert(key.length == len,
      "Provided key has incorrect length: " + key.length + ", expected " + len)
    assert(key.head != '*',
      "Cannot perform wildcard search for value (key = " + key + ").")

    // get the mapping bucket
    val bucket = cToI(key.head)
    val next = key.drop(1)

    // fold over bucket
    prefixMap(bucket).fold(default)(_.getOrElse(next, default))
  }

  def get(key: String): V = {
    val rv = getIfExists(key)

    // check for defined return value
    if (rv.isDefined) {
      rv.get
    } else {
      throw new IllegalArgumentException("Key " + key + " is not defined.")
    }
  }

  private[prefixtrie] def find(key: String): TraversableOnce[(String, V)] = {
    // get the mapping bucket
    val bucket = cToI(key.head)
    val next = key.drop(1)

    // do we need to search all buckets? if we do, don't filter anything, else only filter on key
    (0 to 3).filter(i => bucket == -1 || bucket == i)
      .flatMap(i => {
        // get prepension character
        val c = iToC(i)

        // fold to search, and map to prepend
        prefixMap(i).fold(Iterator[(String, V)]()
          .asInstanceOf[TraversableOnce[(String, V)]])(_.find(next))
          .map(kv => (c + kv._1, kv._2))
      })
  }

  def search(key: String): Map[String, V] = {
    assert(key.length == len,
      "Provided key has incorrect length: " + key.length + ", expected " + len)

    // call to internal find method
    find(key).toMap
  }

  def suffixSearch(key: String): Map[String, V] = {
    assert(key.length <= len,
      "Key (" + key + ") must be less than " + len + " characters long.")

    // append a prefix of wildcards and search
    search(("*" * (len - key.length)) + key)
  }

  def prefixSearch(key: String): Map[String, V] = {
    assert(key.length <= len,
      "Key (" + key + ") must be less than " + len + " characters long.")

    // prepend a suffix of wildcards and search
    search(key + ("*" * (len - key.length)))
  }

  def size: Int = prefixMap.map(_.fold(0)(_.size)).fold(0)(_ + _)
}

/**
 * This class comprises the "lowest" level of our fixed depth trie, and therefore contains the
 * values stored in the trie. Scaladoc is only included for the methods unique to this class,
 * otherwise see docs for the DNAPrefixTrie trait.
 *
 * @tparam V Type of the values stored by this class.
 *
 * @see DNAPrefixTrie
 */
private[prefixtrie] class FixedDepthDNAPrefixTrieValues[V](init: Map[String, V]) extends DNAPrefixTrie[V] {

  // filter out ambiguous bases
  protected val values = init.filter(kv => (kv._1 == "A" ||
    kv._1 == "C" ||
    kv._1 == "G" ||
    kv._1 == "T"))

  def contains(key: String): Boolean = values.contains(key)

  def get(key: String): V = values(key)

  def getOrElse(key: String, default: V): V = values.getOrElse(key, default)

  def getIfExists(key: String): Option[V] = {
    if (values.contains(key)) {
      Some(values(key))
    } else {
      None
    }
  }

  private[prefixtrie] def find(key: String): TraversableOnce[(String, V)] = {
    if (key == "*" || key == "N") {
      values
    } else {
      values.filterKeys(_ == key)
    }.toIterator
  }

  def search(key: String): Map[String, V] = {
    assert(key.length == 1,
      "Key (" + key + ") must have length 1.")

    find(key).toMap
  }

  /**
   * Performs a {pre,suf}-fix search on this trie. Since this level of the tree only stores
   * keys of length 1, the two operations are identical. If keys of length 0 are provided,
   * we perform a wildcard search.
   *
   * @param key Key to search for.
   * @tparam V Type of objects in this tree.
   * @return Returns the result of searching for this key at this level of the tree.
   */
  private def fixSearch(key: String): Map[String, V] = {
    if (key.length == 1) {
      find(key).toMap
    } else if (key.length == 0) {
      values
    } else {
      throw new IllegalArgumentException("Key (" + key + ") must have length 0 or 1.")
    }
  }

  // prefix and suffix search are identical for this tree, since we only store keys of length 1
  def prefixSearch(key: String): Map[String, V] = fixSearch(key)
  def suffixSearch(key: String): Map[String, V] = fixSearch(key)

  def size: Int = values.size
}
