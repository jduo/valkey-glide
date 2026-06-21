FROM --platform=linux/amd64 gradle:8.5-jdk11 AS build
WORKDIR /src
COPY build.gradle settings.gradle ./
COPY glide-local.jar ./
COPY src ./src
RUN gradle --no-daemon fatJar -q

FROM --platform=linux/amd64 amazoncorretto:11.0.18-al2
COPY --from=build /src/build/libs/test.jar /test.jar
ENTRYPOINT ["java", \
  "-Xmx1g", "-Xms1g", \
  "-XX:MaxDirectMemorySize=96m", \
  "-XX:+UseG1GC", \
  "-jar", "/test.jar"]
