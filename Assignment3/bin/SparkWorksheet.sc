import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * In order to use this in IntelijIdea go to
  * Preferences -> Languages & Frameworks -> Scala -> Worksheets tab
  * And let only "use eclipse comptatibility mode" be ticked and also
  * if you like "run worksheet in the interactive mode"
  *
  * Tested only in Intelij 2018.1.5
  */
object SparkWorksheet {

  println("Initializing Spark...")
  Logger.getLogger("org").setLevel(Level.OFF)
  val conf = new SparkConf().setMaster("local[*]").setAppName("SparkWorksheet")
  val sc = new SparkContext(conf)
  println("Done")

  /** **Init Data** **/
  val numbers = List(1 -> 1, 2 -> 4, 3 -> 9)
  val letters = List(1 -> List("a", "b", "c"), 2 -> List("d", "e"), 3 -> List("c", "d"))

  val numbersRDD = sc.parallelize(numbers)
  val lettersRDD = sc.parallelize(letters)

  /** ** Join** **/
  val numbersJoinLettersRDD = numbersRDD.join(lettersRDD)
  val numbersJoinLetters = numbersJoinLettersRDD.collectAsMap()

  /** **MapValues** **/
  //val mappedValuesRDD = numbersJoinLettersRDD.mapValues(x=>x._2.map(a=>x._1->a))
  val mappedValuesRDD = numbersJoinLettersRDD.mapValues { case (nr, letters) => letters.map(letter => letter -> nr) }
  val mappedValues = mappedValuesRDD.collect().toMap

  /** **FlatMap** **/
  val flatMappedRDD = numbersJoinLettersRDD.flatMap { case (k, (nr, letters)) => letters.map(_ -> nr).toMap.updated(k.toString, -7) }
  val flatMapped = flatMappedRDD.collect()

  /** **Data-2** **/
  val oneToSix = List(1, 2, 3, 4, 5, 6)
  val sixToEight = List(6, 7, 8)
  val oneToSixRdd = sc.parallelize(oneToSix)
  val sixToEightRDD = sc.parallelize(sixToEight)

  /** **Union** **/

  val oneToEightRDD = oneToSixRdd.union(sixToEightRDD)
  val oneToEight = oneToEightRDD.collect()

  //stop spark
  sc.stop()
}