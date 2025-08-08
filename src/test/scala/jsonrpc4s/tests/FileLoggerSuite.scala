package jsonrpc4s.tests

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scribe.file.FileWriter

class FileLoggerSuite extends AnyWordSpec with Matchers {
  "logs" should {
    "not go to stdout" in {
      val path = Files.createTempFile("lsp4s", ".log")
      val baos = new ByteArrayOutputStream()

      val writer = new FileWriter(path).flushAlways
      Console.withOut(new PrintStream(baos)) {
        val logger = scribe.Logger("lsp4s").orphan().withHandler(writer = writer)
        logger.info("This is info")
        logger.warn("This is warning")
        logger.error("This is error")
      }
      val obtainedOut = baos.toString()
      obtainedOut shouldBe empty
      val obtainedLogs =
        new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
      List("info", "warning", "error").foreach { message =>
        obtainedLogs should include(s"This is $message")
      }
    }
  }
}
