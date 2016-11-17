FROM openjdk:8-jre

WORKDIR /usr/share/netty-queue

RUN mkdir -p data
RUN mkdir -p bin
RUN mkdir -p lib

COPY src/main/bin/netty-queue.sh ./bin/
COPY target/netty-queue.jar ./lib/

RUN chmod +x ./bin/netty-queue.sh

ENV PATH /usr/share/netty-queue/bin:$PATH

EXPOSE 8800 8900

CMD ["bin/netty-queue.sh"]