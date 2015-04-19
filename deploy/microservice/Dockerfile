FROM ubuntu:14.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y openjdk-7-jdk

# config, mainjar and logs folders respectively
RUN mkdir /etc/toktok/; mkdir /usr/local/toktok; mkdir /var/log/toktok
RUN chmod 0775 /etc/toktok; chmod 0775 /usr/local/toktok; chmod 0775 /var/log/toktok/

ENV configfolder /etc/toktok/
ENV mainclass @MAINCLASS
ENV mainjar /usr/local/toktok/@SERVICE-assembly-@VERSION.jar
ENV cp $configfolder:$mainjar
#ENV javaoptions=''

ADD @SERVICE/target/scala-@SCALA/@SERVICE-assembly-@VERSION.jar /usr/local/toktok/@SERVICE-assembly-@VERSION.jar

CMD java -cp $cp $mainclass
