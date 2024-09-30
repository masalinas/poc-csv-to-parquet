# Description

Convert any csv file to parquet file format in java using the last SpringBoot version 3.3.x with the last spark version 4.0.0.x. In this case we must to update the servlet version 6.x used by Spring with the previous one 5.x used by Spark using the property called **jakarta-servlet.version** with the value 5.0.0. Check the pom file of the project.

## Test your API

To convert a csv filr to parquet file using spark we can execute this endpoint and see the results:

```
curl -F "file=@/home/miguel/temp/datasets/sample/datamatrix.csv" http://localhost:8888/convert
```