package com.scala.learn;

class Variable {

  def function_variable() = {

    print("Hello Welcome to Quvid Technologies")
  }


  def function_greeting(name:String, age:Int): String ={
     "My Name is "+name+" and I am "+age+" years old"
  }


  def fibonacci(i:Int): Int = {
    if(i <=  1) {
      return i;
    }else{
      return  fibonacci(i-1) + fibonacci(i-2)
    }
  }

  def prime_number(i:Int): Boolean ={

    for( w <- 0 to i){
      if(i / w == 0){
          return true
        }else{
          return false
        }
    }
    return false
  }

}


object variable_obj extends Variable {

  def main(args: Array[String]): Unit = {
      //function_variable()
      //print(function_greeting("Dinesh",38))
      //print(fibonacci(6))

      println(prime_number(1))
      println(prime_number(3))
      println(prime_number(10))
      println(prime_number(19))
  }

}