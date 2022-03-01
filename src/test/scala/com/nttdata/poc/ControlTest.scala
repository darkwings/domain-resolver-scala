package com.nttdata.poc

import org.scalatest.wordspec.AnyWordSpec
import Control._

class Mock() {
  var closed = false
  def close():Unit = closed = true

  def doSomething(v: Int): String = v.toString
}

class ControlTest extends AnyWordSpec{

  "Control" should {
    "close resource" in {
      val m = new Mock() // Mock include close():Unit method
      val s = using(m) {_.doSomething(1)}
      assert(s == "1")
      assert(m.closed) // Close is called by Control.using
    }
  }
}
