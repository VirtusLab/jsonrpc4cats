package jsonrpc4s.tests

import cats.effect.IO
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scribe.Logger
import jsonrpc4s.Endpoint
import jsonrpc4s.Services
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig

class ServicesSuite extends AnyWordSpec with Matchers {
  implicit val intCodec: JsonValueCodec[Int] = JsonCodecMaker.make(CodecMakerConfig)

  "duplicate method" should {
    "throw IllegalArgumentException" in {
      val duplicate = Endpoint.notification[Int]("duplicate")
      val base = Services.empty[IO](Logger.root).notification(duplicate)(_ => ())
      an[IllegalArgumentException] should be thrownBy {
        base.notification(duplicate)(_ => ())
      }
    }
  }
}
