mvn clean compile package
#execute the script: ./run.sh <file path of test file>
spark-submit --master local target/spark-scala-1.0-SNAPSHOT.jar $1
