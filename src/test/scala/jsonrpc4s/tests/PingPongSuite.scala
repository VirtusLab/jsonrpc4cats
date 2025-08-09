package jsonrpc4s.tests

import weaver._
import cats.effect.IO
import cats.effect.{Deferred, Ref}
import scribe.Logger
import jsonrpc4s.Endpoint
import jsonrpc4s.Services
import jsonrpc4s.RpcClient
import jsonrpc4s.Service
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
    for {
      pongsRef <- Ref.of[IO, List[String]](Nil)
      done <- Deferred[IO, Unit]

      baseServices = Services
        .empty[IO](Logger.root)
        .request(Hello) { msg => s"$msg, World!" }
        .notificationAsync[String](Pong)(new Service[IO, String, Unit] {
          def handle(message: String): IO[Unit] = {
            println(s"!!!!! Received Pong: $message")
            for {
              _ <- pongsRef.update(messages => message :: messages)
              size <- pongsRef.get.map(_.size)
              _ <- IO.println(s"!!!!! Pongs size: $size")
              _ <- if (size == 2) done.complete(()).void *> IO.println("!!!!! Done") else IO.unit
            } yield ()
          }
        })

      pongBack = { (client: RpcClient[IO]) =>
        baseServices.notificationAsync[String](Ping)(new Service[IO, String, Unit] {
          def handle(message: String): IO[Unit] = {
            println(s"!!!!! Received Ping: $message")
            val pongMessage = message.replace("Ping", "Pong")
            client.notify(Pong, pongMessage)
          }
        })
      }

      result <- TestConnection(pongBack, pongBack).use { conn =>
        println(s"Starting test with connection ${conn}")
        for {
          _ <- conn.alice.client.notify(Ping, "Ping from client")
          _ <- IO.println("Nofified from client")
          _ <- conn.bob.client.notify(Ping, "Ping from server")
          _ <- IO.println("Nofified from server")
          response <- {
            val headers = Map("Custom-Header" -> "Custom-Value")
            conn.alice.client.request(Hello, "Hello", headers)
          }
          _ <- IO.println("Received response for hello")
          _ <- done.get
          _ <- IO.println("Done")
          pongs <- pongsRef.get
          _ <- IO.println(s"Pongs: $pongs")
        } yield {
          println(s"!!!!! Response: $response")
          val helloExpectation = response match {
            case RpcSuccess(helloWorld, _) => expect(helloWorld == "Hello, World!")
            case RpcFailure(methodName, error) => failure(s"Request failed: $methodName - $error")
          }

          val obtainedPongs = pongs.sorted
          val expectedPongs = List("Pong from client", "Pong from server")
          val pongsExpectation = expect(obtainedPongs == expectedPongs)

          val ex = helloExpectation && pongsExpectation
          println(s"!!!!! Ex: $ex")
          ex
        }
      }
    } yield result
  }
}
