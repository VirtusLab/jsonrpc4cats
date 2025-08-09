package jsonrpc4s.tests

import weaver._
import cats.effect.IO
import scribe.Logger
import jsonrpc4s.Endpoint
import jsonrpc4s.Services
import jsonrpc4s.RpcClient
import jsonrpc4s.testkit.TestConnection
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig
import jsonrpc4s.{RpcSuccess, RpcFailure}

/**
 * Tests the following sequence:
 *
 * Alice     Bob
 * =============
 * |  Ping   |
 * | ------> |
 * |  Pong   |
 * | <------ |
 * |         |
 * |  Ping   |
 * | <------ |
 * |  Pong   |
 * | ------> |
 * |         |
 * |  Hello  |
 * | ----->> |
 * | <<<---- |
 *
 * Where:
 * ---> indicates notification
 * -->> indicates request
 * ->>> indicates response
 */
object PingPongSuite extends SimpleIOSuite {

  implicit val stringCodec: JsonValueCodec[String] = JsonCodecMaker.make(CodecMakerConfig)
  private val Ping = Endpoint.notification[String]("ping")
  private val Pong = Endpoint.notification[String]("pong")
  private val Hello = Endpoint.request[String, String]("hello")

  test("ping pong should work correctly") {
    val services = Services
      .empty[IO](Logger.root)
      .request(Hello) { msg => s"$msg, World!" }

    val pongBack: RpcClient[IO] => Services[IO] = { _ => services }

    TestConnection(pongBack, pongBack)
      .use { conn =>
        for {
          response <- {
            val headers = Map("Custom-Header" -> "Custom-Value")
            conn.alice.client.request(Hello, "Hello", headers)
          }
        } yield {
          response match {
            case RpcSuccess(helloWorld, _) =>
              expect(helloWorld == "Hello, World!")
            case RpcFailure(methodName, error) =>
              failure(s"Request failed: $methodName - $error")
          }
        }
      }
  }
}
