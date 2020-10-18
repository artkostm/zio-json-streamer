lazy val json_streamer = (project in file(".")).settings(
  name := "k-anonimity",
  version := "1.0",
  scalaVersion := "2.12.10",
  libraryDependencies ++= Seq(
    "dev.zio" %% "zio" % "1.0.0-RC18-2",
    "dev.zio" %% "zio-streams" % "1.0.0-RC18-2",
    "com.github.jsurfer" %% "jsurfer-jackson" % "1.6.0"
  )
)