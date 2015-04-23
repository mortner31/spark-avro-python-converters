# spark-avro-python-converters
This is a fork from spark python examples, where I extracted what  needed to read / write from avro files over hdfs

Just for the record : I am an experienced C, C++, python developer, but I have absolutely no experienece in Java / scala. 
I therefore know nothing in scala or  maven. This set of files is *very dirty*.

I wanted to have a self containted package for reading and writing avro file, I tweaked spark source directory. 
Hence Spark licence applies here, This is basically a rough extract from spark source code. 

I used a pulled request in spark from staslos : 
https://github.com/staslos/spark/commit/ef026be7981c6d892e2d2e35e8b100c9def2dd6a

And this was inspired by this topic : 
http://stackoverflow.com/questions/29619081/python-spark-error-writing-to-avro


To build : 

```
mvn -DskipTests package
```

To check what is insided compiled packages 

```
jar tf target/AvroToPythonConverters-1.3.0.jar
```

For using, I have setup a demo file :  

```
spark-submit --driver-class-path target/AvroToPythonConverters-1.3.0.jar demo.py
```





