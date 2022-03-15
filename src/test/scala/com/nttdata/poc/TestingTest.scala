package com.nttdata.poc

import org.scalatest.wordspec.AnyWordSpec

case class T(timestamp: String) {

  def optTimestamp(): Option[String] = Option(timestamp)
}

class TestingTests extends AnyWordSpec {

  "Manage null" should {
    "Java null should be managed" in {
      val nulled: T = null
      val notNull: T = T("2022-03-15 12:12:00.000000Z")
      val nullWrapped:T = T(null)

      val o = Option(nulled)
      assert(o == None)

      val o2 = Option(nullWrapped).flatMap(b => b.optTimestamp())
      assert(o2 == None)

      val o3 = Option(notNull).flatMap(b => b.optTimestamp())
      assert(o3 == Some("2022-03-15 12:12:00.000000Z"))
    }
  }
}
