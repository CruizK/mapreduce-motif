export JAVA_HOME=/usr/lib/jvm/java-8-openjdk/jre
export HADOOP_CLASSPATH=/usr/lib/jvm/java-8-openjdk/lib/tools.jar
alias hadoop="/usr/local/hadoop/bin/hadoop"
rm -rf output
hadoop com.sun.tools.javac.Main MotifSearch.java -target 1.8
jar cf motif.jar MotifSearch*.class
hadoop jar motif.jar MotifSearch input output
