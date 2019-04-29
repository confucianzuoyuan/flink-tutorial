package com.atguigu.day1

trait Parsers[ParseError, Parser[+_]] {
  def run[A](p: Parser[A])(input: String): Either[ParseError, A]
  def char(c: Char): Parser[Char]
}

object Test {
  def main(args: Array[String]): Unit = {
    val arr = "sensor_id".split("_")
    arr.foreach(println)
  }
}