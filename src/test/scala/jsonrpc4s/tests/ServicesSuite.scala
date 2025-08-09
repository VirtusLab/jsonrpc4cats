package jsonrpc4s.tests

import cats.effect.IO
import weaver._
import scribe.Logger
import jsonrpc4s.Endpoint
import jsonrpc4s.Services
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig

object ServicesSuite extends FunSuite {
  implicit val intCodec: JsonValueCodec[Int] = JsonCodecMaker.make(CodecMakerConfig)

  test("duplicate method should throw IllegalArgumentException") {
    val duplicate = Endpoint.notification[Int]("duplicate")
    val base = Services.empty[IO](Logger.root).notification(duplicate)(_ => ())

    expect {
      try {
        base.notification(duplicate)(_ => ())
        false
      } catch {
        case _: IllegalArgumentException => true
        case _: Throwable => false
      }
    }
  }
}
