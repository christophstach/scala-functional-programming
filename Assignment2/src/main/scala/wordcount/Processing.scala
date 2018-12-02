package wordcount

class Processing {

  /**
    * ********************************************************************************************
    *
    * Aufgabe 1
    *
    * ********************************************************************************************
    */
  def getWords(line: String): List[String] = {
    /*
     * Extracts all words from a line
     * 
     * 1. Removes all characters which are not letters (A-Z or a-z)
     * 2. Shifts all words to lower case
     * 3. Extracts all words and put them into a list of strings
     */
    line
      .replaceAll("[^A-Za-z]", " ")
      .toLowerCase()
      .split(" ")
      .filter(w => !w.equals(""))
      .toList
  }

  def getAllWords(l: List[(Int, String)]): List[String] = {
    /*
     * Extracts all words from a List containing line number and line tuples
     * The words should be in the same order as they occur in the source document
     * 
     * Hint: Use the flatMap function
     */
    l
      .map(tuple => tuple._2)
      .flatMap(getWords)
  }

  def countWords(l: List[String]): List[(String, Int)] = {
    /*
     *  Gets a list of words and counts the occurrences of the individual words
     */
    l
      .flatMap(getWords)
      .groupBy(w => w)
      .map(kv => (kv._1, kv._2.size))
      .toList

  }

  /**
    * ********************************************************************************************
    *
    * Aufgabe 2
    *
    * ********************************************************************************************
    */

  def mapReduce[S, B, R](mapFun: S => B, redFun: (R, B) => R, base: R, l: List[S]): R =
    l.map(mapFun).foldLeft(base)(redFun)

  def countWordsMR(l: List[String]): List[(String, Int)] = {
    //mapReduce[???,???,???](null,null,null,l)

    mapReduce[String, (String, Int), List[(String, Int)]](
      word => (word, 1),
      (acc, curr) => {
        if (acc.contains(curr)) acc.map(tuple => if (tuple._1 == curr._1) (tuple._1, tuple._2 + curr._2) else tuple)
        else acc ++ List(curr)
      },
      List[(String, Int)](),
      l
    )
  }


  /**
    * ********************************************************************************************
    *
    * Aufgabe 3
    *
    * ********************************************************************************************
    */

  def getAllWordsWithIndex(l: List[(Int, String)]): List[(Int, String)] = {
    l.flatMap(numberLineTuple => {
      getWords(numberLineTuple._2).map(word => {
        (numberLineTuple._1, word)
      })
    })
  }

  def createInverseIndex(l: List[(Int, String)]): Map[String, List[Int]] = {
    l.foldLeft(Map[String, List[Int]]())((acc, curr) => {
      if (acc.contains(curr._2)) acc.updated(curr._2, acc(curr._2) ++ List(curr._1))
      else acc + (curr._2 -> List(curr._1))
    })
  }

  def orConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {
    words.map(word => {
      if (invInd.contains(word)) invInd(word)
      else List()
    }).reduceLeft((acc, curr) => acc.union(curr))
  }

  def andConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {
    words.map(word => {
      if (invInd.contains(word)) invInd(word)
      else List()
    }).reduceLeft((acc, curr) => acc.intersect(curr))
  }
}


object Processing {

  def getData(filename: String): List[(Int, String)] = {
    val url = getClass.getResource("/" + filename).getPath
    val src = scala.io.Source.fromFile(url)
    val iter = src.getLines()
    var c = -1
    val result = (for (row <- iter) yield {
      c = c + 1; (c, row)
    }).toList
    src.close()
    result
  }
}