package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection._
import org.apache.spark.mllib.linalg.distributed.MatrixEntry

@SerialVersionUID(123L)
case class M_Matrix ( i: Long, j: Long, v: Double )
extends Serializable {}

@SerialVersionUID(123L)
case class N_Matrix ( j: Long, k: Long, w: Double )
extends Serializable {}


object Multiply {
  def main(args: Array[ String ]) {

val conf = new SparkConf().setAppName("Multiply")
val sc = new SparkContext(conf)

val M_ = sc.textFile(args(0)).map{ line =>
  val a = line.split(",")
  MatrixEntry(a(0).toLong, a(1).toLong, a(2).toDouble)
}

val N_ = sc.textFile(args(1)).map{ line =>
  val a = line.split(",")
  MatrixEntry(a(0).toLong, a(1).toLong, a(2).toDouble)
}

val res = M_.map( M_ => (M_.j,M_) )
    .join(N_.map( N_ => (N_.i, N_)))
    .map{ case (_, (m_matrix, n_matrix)) => ((m_matrix.i, n_matrix.j), m_matrix.value * n_matrix.value)}
    .reduceByKey(_ + _)
    .map{ case ((i, k), sum) => MatrixEntry(i, k, sum)}

res.collect().foreach(println)
 }
}
