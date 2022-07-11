package com.atguigu.Utils

import java.util.ResourceBundle

object Properties {


  def apply(key:String)={
    val bundle: ResourceBundle = ResourceBundle.getBundle("conf")
    bundle.getString(key)
  }

  def main(args: Array[String]): Unit = {
    println(Properties("KAFKA_TOPIC"))
  }

}
