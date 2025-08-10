package jsonrpc4s.tests

import weaver._
import cats.effect.IO
import cats.effect.{Deferred, Ref}
import cats.effect.kernel.Outcome
import scribe.Logger
import jsonrpc4s.Endpoint
import jsonrpc4s.Services
import jsonrpc4s.RpcClient
import jsonrpc4s.Service
import jsonrpc4s.testkit.TestConnection
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig
import jsonrpc4s.{RpcSuccess, RpcFailure, ErrorCode}

object EndToEndSuite extends SimpleIOSuite {

  implicit val stringCodec: JsonValueCodec[String] = JsonCodecMaker.make(CodecMakerConfig)
  implicit val intCodec: JsonValueCodec[Int] = JsonCodecMaker.make(CodecMakerConfig)

  private val Ping = Endpoint.notification[String]("ping")
  private val Pong = Endpoint.notification[String]("pong")
  private val Hello = Endpoint.request[String, String]("hello")

  test("ping/pong notifications and request should work correctly") {
    for {
      pongsRef <- Ref.of[IO, List[String]](Nil)
      done <- Deferred[IO, Unit]

      baseServices = Services
        .empty[IO](Logger.root)
        .request(Hello) { msg => s"$msg, World!" }
        .notificationAsync[String](Pong)(new Service[IO, String, Unit] {
          def handle(message: String): IO[Unit] =
            for {
              _ <- pongsRef.update(messages => message :: messages)
              size <- pongsRef.get.map(_.size)
              _ <- if (size == 2) done.complete(()).void else IO.unit
            } yield ()
        })

      pongBack = { (client: RpcClient[IO]) =>
        baseServices.notificationAsync[String](Ping)(new Service[IO, String, Unit] {
          def handle(message: String): IO[Unit] = {
            val pongMessage = message.replace("Ping", "Pong")
            client.notify(Pong, pongMessage)
          }
        })
      }

      result <- TestConnection(pongBack, pongBack).use { conn =>
        for {
          _ <- conn.alice.client.notify(Ping, "Ping from client")
          _ <- conn.bob.client.notify(Ping, "Ping from server")
          response <- conn.alice.client
            .request(Hello, "Hello", Map("Custom-Header" -> "Custom-Value"))
          _ <- done.get
          pongs <- pongsRef.get
        } yield {
          val helloExpectation = response match {
            case RpcSuccess(helloWorld, _) => expect(helloWorld == "Hello, World!")
            case RpcFailure(methodName, error) => failure(s"Request failed: $methodName - $error")
          }

          val obtainedPongs = pongs.sorted
          val expectedPongs = List("Pong from client", "Pong from server")
          val pongsExpectation = expect(obtainedPongs == expectedPongs)

          helloExpectation && pongsExpectation
        }
      }
    } yield result
  }

  test("request to unknown method should return MethodNotFound error") {
    val Unknown = Endpoint.request[String, String]("unknownMethod")
    TestConnection[IO](_ => Services.empty[IO](Logger.root), _ => Services.empty[IO](Logger.root))
      .use { conn =>
        conn.alice.client.request(Unknown, "hi").map {
          case RpcFailure(_, err) => expect(err.error.code == ErrorCode.MethodNotFound)
          case RpcSuccess(_, _) => failure("Expected MethodNotFound error, got success")
        }
      }
  }

  test("server handler failure should return InternalError") {
    val Boom = Endpoint.request[String, String]("boom")
    val serverServices: RpcClient[IO] => Services[IO] = _ =>
      Services
        .empty[IO](Logger.root)
        .requestAsync(Boom)(new Service[IO, String, String] {
          def handle(request: String): IO[String] = IO.raiseError(new RuntimeException("boom"))
        })

    TestConnection[IO](_ => Services.empty[IO](Logger.root), serverServices).use { conn =>
      conn.alice.client.request(Boom, "go").map {
        case RpcFailure(_, err) =>
          expect(err.error.code == ErrorCode.InternalError) && expect(
            err.error.message.contains("boom")
          )
        case RpcSuccess(_, _) => failure("Expected InternalError, got success")
      }
    }
  }

  test("client-side cancellation should cancel server request") {
    val Slow = Endpoint.request[Int, Int]("slow-op")
    for {
      startedOnServer <- Deferred[IO, Unit]
      cancelledOnServer <- Deferred[IO, Unit]

      serverServices = { (_: RpcClient[IO]) =>
        Services
          .empty[IO](Logger.root)
          .requestAsync(Slow)(new Service[IO, Int, Int] {
            def handle(n: Int): IO[Int] =
              startedOnServer.complete(()).void *>
                IO.never.onCancel(
                  cancelledOnServer.complete(()).void
                )
          })
      }

      result <- TestConnection[IO](_ => Services.empty[IO](Logger.root), serverServices).use {
        conn =>
          for {
            fiber <- cats.effect.kernel.Async[IO].start(conn.alice.client.request(Slow, 42))
            _ <- startedOnServer.get
            _ <- fiber.cancel
            _ <- cancelledOnServer.get
            outcome <- fiber.join
          } yield expect(outcome == Outcome.Canceled())
      }
    } yield result
  }
}
