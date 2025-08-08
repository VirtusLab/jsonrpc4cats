package jsonrpc4s.tests

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import jsonrpc4s.ErrorCode
import com.github.plokhotnyuk.jsoniter_scala.core.{readFromArray, writeToArray, writeToString}
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig

class ErrorCodeSuite extends AnyWordSpec with Matchers {

  implicit val intCodec: JsonValueCodec[Int] = JsonCodecMaker.make(CodecMakerConfig)

  def check(code: Int, expected: ErrorCode): Unit = {
    expected.toString should {
      "serialize and deserialize correctly" in {
        val obtained = readFromArray[ErrorCode](writeToArray(code))
        obtained shouldBe expected
        val obtainedJson = writeToString(expected)
        obtainedJson shouldBe code.toString
      }
    }
  }

  check(666, ErrorCode.Unknown(666))
  check(-32000, ErrorCode.Unknown(-32000))
  check(-32099, ErrorCode.Unknown(-32099))
  ErrorCode.builtin.foreach { code =>
    // Check code is same
    check(code.value, code)
  }
}
