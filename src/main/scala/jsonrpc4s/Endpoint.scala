package jsonrpc4s

import cats.effect.kernel.Async
import cats.effect.kernel.Fiber
import scala.concurrent.Future

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig
import com.github.plokhotnyuk.jsoniter_scala.core.JsonReader
import com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter

class Endpoint[A, B](
    val method: String
)(implicit val codecA: JsonValueCodec[A], val codecB: JsonValueCodec[B]) {
  def request[F[_]: Async](
      request: A,
      headers: Map[String, String] = Map.empty
  )(implicit client: RpcActions[F]): F[RpcResponse[B]] = {
    client.request[A, B](this, request, headers)
  }

  def notify[F[_]: Async](
      notification: A,
      headers: Map[String, String] = Map.empty
  )(implicit client: RpcActions[F], ev: B =:= Unit): F[Unit] = {
    // Safe to do because of `ev` proving that B == Unit at compile time
    val safeThis = this.asInstanceOf[Endpoint[A, Unit]]
    client.notify[A](safeThis, notification, headers)
  }
}

object Endpoint {
  def request[A: JsonValueCodec, B: JsonValueCodec](method: String): Endpoint[A, B] =
    new Endpoint(method)
  def notification[A: JsonValueCodec](method: String): Endpoint[A, Unit] =
    new Endpoint(method)

  implicit val unitCodec: JsonValueCodec[Unit] = {
    final case class Empty()
    val empty = Empty()
    val emptyCodec = JsonCodecMaker.make[Empty](CodecMakerConfig)

    new JsonValueCodec[Unit] {
      def decodeValue(in: JsonReader, default: Unit) = emptyCodec.decodeValue(in, empty)
      def encodeValue(x: Unit, out: JsonWriter) = emptyCodec.encodeValue(empty, out)
      def nullValue = ()
    }
  }
}
