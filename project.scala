//> using scala 2.12.19 2.13.13
//> using jvm 8

//> using options -Yrangepos -deprecation -Xlint
//> using options -Xfatal-warnings -Ywarn-unused -Ywarn-unused-import -Ywarn-value-discard

//> using dep "org.typelevel::cats-effect:3.6.3"
//> using dep "co.fs2::fs2-core:3.12.0"
// > using dep "co.fs2::fs2-io:3.12.0"
//> using dep "com.outr::scribe:3.5.5"
// > using dep "com.outr::scribe-cats:3.15.2"

//> using dep "com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-core:2.13.5"
//> using compileOnly.dep "com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-macros:2.13.5"

//> using test.dep "com.outr::scribe-file:3.5.5"
//> using test.dep "com.disneystreaming::weaver-cats:0.8.4"
//> using test.dep "com.lihaoyi::pprint:0.9.3"

//> using publish.organization "io.github.alexarchambault.bleep"
//> using publish.name "jsonrpc4s"
//> using publish.ci.computeVersion "git:tag"
//> using publish.ci.repository "central-s01"
//> using publish.ci.user "env:PUBLISH_USER"
//> using publish.ci.password "env:PUBLISH_PASSWORD"
//> using publish.ci.secretKey "env:PUBLISH_SECRET_KEY"
//> using publish.ci.secretKeyPassword "env:PUBLISH_SECRET_KEY_PASSWORD"
//> using publish.ci.publicKey "env:PUBLISH_PUBLIC_KEY"
//> using publish.license "Apache-2.0"
//> using publish.url "https://github.com/alexarchambault/jsonrpc4s"
//> using publish.versionControl "github:alexarchambault/jsonrpc4s"
//> using publish.developer "Jorge Vicente Cantero||https://jvican.github.io/"
