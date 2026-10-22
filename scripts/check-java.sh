#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ozzy_tools.sh"
cd "$ozzy_repo_root"
ozzy_java_build="$CARGO_TARGET_DIR/java"
ozzy_java_arch="$(uname -m)"
ozzy_java_platform="linux-$ozzy_java_arch"
ozzy_java_profile=debug
ozzy_java_release=()
if [[ "${OZZY_JAVA_RELEASE:-0}" == 1 ]]; then
    ozzy_java_profile=release
    ozzy_java_release=(--release)
fi
./scripts/ozzy_cargo build -p ozzy-java "${ozzy_java_release[@]}" --locked
mkdir -p "$ozzy_java_build/native-resources/io/ozzy/native/$ozzy_java_platform"
cp "$CARGO_TARGET_DIR/$ozzy_java_profile/libozzy_java.so" \
    "$ozzy_java_build/native-resources/io/ozzy/native/$ozzy_java_platform/"
mvn -B -f bindings/java/pom.xml "-Dozzy.buildDir=$ozzy_java_build" \
    "-Dozzy.platform=$ozzy_java_platform" package
mkdir -p "$ozzy_java_build/javadoc" "$ozzy_java_build/smoke"
find bindings/java/src/main/java -name '*.java' > "$ozzy_java_build/sources.list"
javadoc -quiet -Xdoclint:all,-missing -Werror --release 22 \
    -d "$ozzy_java_build/javadoc" @"$ozzy_java_build/sources.list"
ozzy_java_version="$(./scripts/ozzy_cargo metadata --no-deps --format-version 1 --offline | \
    jq -r '.packages[] | select(.name == "ozzy-java") | .version')"
export OZZY_JAVA_JAR="$ozzy_java_build/ozzy-java-$ozzy_java_version-$ozzy_java_platform.jar"
export OZZY_JAVA_SMOKE="$ozzy_java_build/smoke"
javac --release 22 -Xlint:all -Werror --class-path "$OZZY_JAVA_JAR" \
    -d "$OZZY_JAVA_SMOKE" bindings/java/src/test/java/io/ozzy/smoke/Smoke.java
./scripts/ozzy_cargo nextest run -p ozzy-java --test-threads 4
./scripts/ozzy_cargo nextest run -p ozzy-java --features package-tests \
    --test ozzy_java_package --profile extended --test-threads 1
