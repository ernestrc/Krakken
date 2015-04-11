FROM ubuntu:14.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y openjdk-8-jdk

# config, mainjar and logs folders respectively
RUN mkdir /etc/toktok/; mkdir /usr/local/toktok; mkdir /var/log/toktok
RUN chmod 0775 /etc/toktok; chmod 0775 /usr/local/toktok; chmod 0775 /var/log/toktok/

EXPOSE 9876

ENV scala_version 2.11
ENV configfolder /etc/toktok/
ENV mainclass io.toktok.Boot
ENV mainjar /usr/local/toktok/toktok-api.jar
ENV cp $configfolder:$mainjar
#ENV javaoptions=''

ADD ./target/scala-${scala_version}/toktok-api.jar /usr/local/toktok/toktok-api.jar

CMD java -cp $cp $mainclass
