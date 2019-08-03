
~/apps/spark-1.6.3-bin-hadoop2.6/bin/spark-submit --master local[4] --class com.dtdream.ecd.spark.app.HiveMetaInfoApp target/scala-2.10/spark-1.6-app-assembly-1.0.jar $1

~/apps/spark-1.6.3-bin-hadoop2.6/bin/spark-submit --master local[4] --class com.dtdream.ecd.spark.app.QualityBaseScoreApp target/scala-2.10/spark-1.6-app-assembly-1.0.jar $1

~/apps/spark-1.6.3-bin-hadoop2.6/bin/spark-submit --master local[4] --class com.dtdream.ecd.spark.app.QualityCalcScoreApp target/scala-2.10/spark-1.6-app-assembly-1.0.jar $1

~/apps/spark-1.6.3-bin-hadoop2.6/bin/spark-submit --master local[4] --class com.dtdream.ecd.spark.app.AssetValueModelApp target/scala-2.10/spark-1.6-app-assembly-1.0.jar $1

cd /Users/elainetuang/Git/dtvengeance
spark-submit target/scala-2.11/dtvengeance-assembly-0.1.0.jar Hive2MySQLTask
