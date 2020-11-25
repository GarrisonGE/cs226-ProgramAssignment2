mvn clean compile package

# hard code file path, execute the script: ./run.sh
#spark-submit --master local target/spark-scala-1.0-SNAPSHOT.jar ../nasa.tsv

# run with parameter, execute the script: ./run.sh <file path of test file>
spark-submit --master local target/spark-scala-1.0-SNAPSHOT.jar $1


