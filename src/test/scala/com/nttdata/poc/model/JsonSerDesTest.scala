package com.nttdata.poc.model

import com.google.gson.{Gson, GsonBuilder}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant


class JsonSerDesTest extends AnyWordSpec {

  "JsonSerDes" should {
    "let Activity" should {
      "be serialized upper case" in {
        val activity = Activity("id", "user", "cn", "201", "a@b.it", "101.1.1.0", "www.a.it",
          Location("a", "b", 100.0, 200.0), "message", Instant.now().toString, "action")
        val gson: Gson = new GsonBuilder().create()
        val json = gson.toJson(activity)
        println(json)

        val bytes = JsonSerDes.activity().serializer().serialize("topic", activity)
        val s = new String(bytes)
        assert(s.contains("ACTIVITYID"))
      }
    }
    "let activity enriched" should {
      "evaluate corretcly suspect flag" in {
        val activity = Activity("id", "user", "cn", "201", "a@b.it", "101.1.1.0", "www.a.it",
          Location("a", "b", 100.0, 200.0), "message", Instant.now().toString, "action")
        val activityE1 = ActivityEnriched(activity)
        assert(!activityE1.lookupComplete)

        val activityE2 = ActivityEnriched(activity, suspect = false)
        assert(activityE2.lookupComplete)
      }
    }
  }
}
