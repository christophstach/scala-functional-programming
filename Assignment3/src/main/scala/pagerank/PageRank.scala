package pagerank

import org.apache.spark.rdd.RDD
import pagerank.models.{Link, Page}

import scala.annotation.tailrec

object PageRank {

  /**
    * Computes the pageRank based on the given links.
    *
    * @param links the links
    * @param t     teleportation
    * @param delta minimum value for difference
    * @return pageRanks
    **/
  def computePageRank(links: RDD[(String, Set[String])], t: Double = 0.15, delta: Double = 0.01): RDD[(String, Double)] = {
    val n = links.count()
    val tNorm = t / n
    val ranks = links.mapValues(_ => 1.0 / n)

    @tailrec
    def inner(ranks: RDD[(String, Double)], links: RDD[(String, Set[String])], tNorm: Double, t: Double, delta: Double)
    : RDD[(String, Double)] = {

      //compute the contributions
      val contributions = computeContributions(ranks, links)

      //combine same keys and apply teleportation and damping factors after
      val newRanks = computeNewRanksFromContributions(contributions, tNorm, t)

      //print(newRanks.collect().toList)

      val diff = computeDifference(ranks, newRanks)
      //done, difference is small enough
      if (diff < delta)
        newRanks
      else
        inner(newRanks, links, tNorm, t, delta)
    }

    inner(ranks, links, tNorm, t, delta)
  }

  /**
    * Computes the contributions from the given ranks and links.
    *
    * See the tests and the description in the assignment sheet for more information.
    *
    *
    * HINTs:
    * join and flatMap might be useful
    *
    * - What happens if a page is never linked to? ex. {A->B, B->B}
    * make sure that (A,0) is also in the result, so A doesn't get lost
    *
    * - also pay attention that A->{} should be treated as A->{A} and contribute (A,1)
    *
    */
  def computeContributions(ranks: RDD[(String, Double)], links: RDD[(String, Set[String])]): RDD[(String, Double)] = {
    /*links
      .map(link => (link._1, if (link._2.nonEmpty) link._2 else Set(link._1)))
      .join(ranks)
      .map(link => (link._1, link._2._1.map(l => (l, link._2._2 * 1.0 / link._2._1.toList.length.toDouble))))
      .map(link => link._2)
      .flatMap(link => link)


    .map(x=>{println(x);x})

  .map(x=>{println(x);math.abs(x._2)})
//     .map(tuplprie => (tuple._1, tuple._2.ceil.toDouble))
      //.filter(tuple => !(Math.abs(tuple._2) <= 0.1))
      */
    /*val contributions = links
      .map(link => (link._1, if (link._2.nonEmpty) link._2 else Set(link._1)))
      .join(ranks).values.flatMap { case (urls, rank) =>
      val size = urls.size

      urls.map(url => (url, rank / size))
    }*/


    /*println(
      links
        .map(link => (link._1, if (link._2.nonEmpty) link._2 else Set(link._1)))
        .fullOuterJoin(ranks).flatMap { case (urls, rank) =>
        //val size = urls.size
        urls
        //urls.map(url => (url, rank / size))
      }
        .collect()
        .toList
    )*/


    /*ranks.sortBy(_._1)
      .fullOuterJoin(contributions)
      .map(v => (v._1, v._2._2))
      .map(v => (v._1, if (v._2.isEmpty) 0.0 else v._2.get))
      */

    val rank_arr = ranks.keys.collect()
    val link_arr = links.values.collect().flatten.toSet
    links.join(ranks).flatMap {
      case t if t._2._1.isEmpty => rank_arr.filter((key: String) => key.equals(t._1)).map(link => (link, t._2._2))
      case t if !t._2._1.intersect(link_arr).contains(t._1) =>
        t._2._1.flatMap(link => List((link, (1 / t._2._1.size.toDouble) * t._2._2), (t._1, 0.0)))
      case t => t._2._1.map(link => (link, (1 / t._2._1.size.toDouble) * t._2._2))
    }
  }

  /**
    *
    * Computes the new ranks from the contributions
    * The difference is computed the following way in pseudocode:
    *
    * foreach key:
    * - sum its values
    * multiply the values obtained in the previous step by (1-t) and add tNorm
    *
    **/
  def computeNewRanksFromContributions(contributions: RDD[(String, Double)], tNorm: Double, t: Double): RDD[(String, Double)] = {
    contributions.reduceByKey((a, b) => a + b).mapValues(c => (c * (1 - t)) + tNorm)
  }

  /**
    *
    * Computes the difference between the old and new ranks.
    * The difference is computed the following way in pseudocode:
    *
    * foreach key:
    * - obtain the absolute value/modulus of its value from ranks subtracted its value in newRanks
    * sum the values
    *
    **/
  def computeDifference(ranks: RDD[(String, Double)], newRanks: RDD[(String, Double)]): Double = {
    ranks.join(newRanks).mapValues(x => (x._1 - x._2).abs).values.sum()
  }

  /**
    * Extracts all links from the given page RDD. This is a 3 step process:
    *
    * 1. Project the pages in the form of Title -> Set(links.titles)
    * 2. Project all other links in the form link.title -> Set()
    * 3. Merge the 2 using the rdd union operation followed by a reduceByKey
    *
    * This results in all pages, who have links to be a pair of pageTitle -> Set (linkTitle, linkTitle..)
    * and all pages, who don't have any links in the form of pageTitle -> Set()
    *
    * For some examples see the test cases
    */
  def extractLinksFromPages(pages: RDD[Page]): RDD[(String, Set[String])] = {
    val pages_with_titles = pages.map(page => (page.title, page.links.flatMap(link => Set(link.title)).toSet))
    val pages_without_titles = pages.flatMap(page => page.links.map((link: Link) => (link.title, Set.empty[String])))

    pages_with_titles.union(pages_without_titles).reduceByKey((a, b) => a ++ b)
  }

}