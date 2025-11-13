# messaging-rabbit

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: <https://quarkus.io/>.

## Project Overview

This repository hosts a simple end-to-end messaging example built on RabbitMQ. Invoking the `POST /messaging` endpoint with a JSON body that includes `title`, `description`, and `event` pushes the payload onto a queue. A lightweight Python service living under `consumer-python/` (run it with `python3 listener.py` after installing `pika`) consumes the message, performs a minimal "receive and forward" processing step, and publishes the enriched payload. The Quarkus application listens for this response on another queue via `src/main/java/br/com/dogdytech/MessagingConsumer.java`, completing the round trip.

- `docker-compose.yaml` bootstraps RabbitMQ so both applications have a broker to talk to during development.
- The Python consumer depends on `pika`; install it with `pip install -r consumer-python/requirements.txt` (or directly with `pip install pika`) before starting the listener.

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./mvnw compile quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at <http://localhost:8080/q/dev/>.

## Packaging and running the application

The application can be packaged using:

```shell script
./mvnw package
```

It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:

```shell script
./mvnw package -Dquarkus.package.jar.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

## Creating a native executable

You can create a native executable using:

```shell script
./mvnw package -Dnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using:

```shell script
./mvnw package -Dnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/messaging-rabbit-1.0.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult <https://quarkus.io/guides/maven-tooling>.

## Related Guides

- REST ([guide](https://quarkus.io/guides/rest)): A Jakarta REST implementation utilizing build time processing and Vert.x. This extension is not compatible with the quarkus-resteasy extension, or any of the extensions that depend on it.

## Provided Code

### REST

Easily start your REST Web Services

[Related guide section...](https://quarkus.io/guides/getting-started-reactive#reactive-jax-rs-resources)
