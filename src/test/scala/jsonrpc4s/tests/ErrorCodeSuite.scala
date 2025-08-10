package jsonrpc4cats.tests

import weaver._
import jsonrpc4cats.ErrorCode
import com.github.plokhotnyuk.jsoniter_scala.core.{readFromArray, writeToArray, writeToString}
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig

object ErrorCodeSuite extends FunSuite {

  implicit val intCodec: JsonValueCodec[Int] = JsonCodecMaker.make(CodecMakerConfig)

  test("ErrorCode 666 serializes and deserializes correctly") {
    val expected = ErrorCode.Unknown(666)
    val obtained = readFromArray[ErrorCode](writeToArray(666))
    val obtainedJson = writeToString[ErrorCode](expected)

    expect(obtained == expected) and
      expect(obtainedJson == "666")
  }

  test("ErrorCode -32000 serializes and deserializes correctly") {
    val expected = ErrorCode.Unknown(-32000)
    val obtained = readFromArray[ErrorCode](writeToArray(-32000))
    val obtainedJson = writeToString[ErrorCode](expected)

    expect(obtained == expected) and
      expect(obtainedJson == "-32000")
  }

  test("ErrorCode -32099 serializes and deserializes correctly") {
    val expected = ErrorCode.Unknown(-32099)
    val obtained = readFromArray[ErrorCode](writeToArray(-32099))
    val obtainedJson = writeToString[ErrorCode](expected)

    expect(obtained == expected) and
      expect(obtainedJson == "-32099")
  }

  // Test builtin error codes
  ErrorCode.builtin.foreach { code =>
    test(s"ErrorCode ${code.value} (${code}) serializes and deserializes correctly") {
      val obtained = readFromArray[ErrorCode](writeToArray(code.value))
      val obtainedJson = writeToString(code)

      expect(obtained == code) and
        expect(obtainedJson == code.value.toString)
    }
  }
}
