(defproject ozzy-jepsen "0.1.0"
  :description "External DQ histories over the production Java SDK"
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.14"]
                 [cheshire "5.13.0"]]
  :main jepsen.ozzy.core
  :test-selectors {:default (complement :integration) :integration :integration}
  :jvm-opts ["--enable-native-access=ALL-UNNAMED" "-Xmx2g" "-Djava.awt.headless=true"
             ~(str "-Djava.io.tmpdir="
                   (or (System/getenv "OZZY_JEPSEN_TMPDIR")
                       (System/getenv "OZZY_ARTIFACT_ROOT") "/tmp"))]
  :resource-paths [~(or (System/getenv "OZZY_JAVA_JAR") "missing-java-jar")]
  :target-path ~(str (or (System/getenv "OZZY_ARTIFACT_ROOT") "/tmp")
                     "/ozzy-jepsen/clojure")
  :local-repo ~(str (or (System/getenv "OZZY_ARTIFACT_ROOT") "/tmp")
                    "/ozzy-jepsen/m2"))
