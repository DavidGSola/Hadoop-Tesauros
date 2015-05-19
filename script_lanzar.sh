javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-0.20-mapreduce/* -d . Tesauros.java 
mv *.class testing_classes/
jar -cvf testing.jar -C testing_classes/ .

hadoop fs -rm -r ./salida_3
hadoop jar testing.jar Tesauros ./datos_testing salida_3