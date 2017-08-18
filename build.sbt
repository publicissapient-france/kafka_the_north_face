resolvers += "confluent" at "http://packages.confluent.io/maven/"

val kafkaVersion = "0.11.0.0"

val sparkVersion = "2.2.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion

libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion exclude("com.fasterxml.jackson.core", "jackson-databind")

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

retrieveManaged := true
