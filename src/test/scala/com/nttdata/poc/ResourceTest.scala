package com.nttdata.poc

import org.scalatest.wordspec.AnyWordSpec

import scala.io.Source

class ResourceTest extends AnyWordSpec{

  "Read resource" should {
    "Read json" in {
      val resource = Source.fromResource("conf-sample/safebrowsing-payload.json").getLines.mkString
      println(resource)
    }
  }
}
