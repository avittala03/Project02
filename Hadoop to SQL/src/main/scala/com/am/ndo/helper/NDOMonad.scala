package com.am.ndo.helper

import org.apache.spark.sql.SparkSession

/**
  * Created by NarsiReddy Avula on 2018-01-25.
  * This is simply a trait with flatMap and map, whose purpose is to allow
  * monad to be used in for expression of the main flow.

  */
trait NDOMonad[A] { current: NDOMonad[A] =>
  def run(spark: SparkSession) : A

  def flatMap[B] (f: A => NDOMonad[B]): NDOMonad[B] =
    new NDOMonad[B] {
      override def run(spark: SparkSession): B = {
        val previousResult: A = current.run(spark)
        val nextMonad: NDOMonad[B] = f(previousResult)
        nextMonad.run(spark)
      }
    }

  def map[B] (f: A => B) : NDOMonad[B] =
    new NDOMonad[B] {
      override def run(spark: SparkSession): B = f(current.run(spark))
    }
}
