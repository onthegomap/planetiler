FROM ubuntu:focal
WORKDIR /
RUN apt update -y && apt install -y openjdk-17-jre-headless libgdal-java && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["java"]
CMD ["-cp", "/run.jar"]
