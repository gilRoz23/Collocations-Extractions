# Collocation-Extraction


### Link to the bucket in S3 
s3://hayon123 

### ARN resource
arn:aws:s3:::hayon123

This application automatically extracts collocations from the Google 2-grams dataset using Amazon Elastic Map Reduce.

## Description

This Map-Reduce program produces the list of top-100 collocations for each decade for English and Hebrew, with their NPMI ratios (in descending order).

## Input

Google Bigrams:

- English: `s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data`
- Hebrew: `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data`

## Algorithm

- N: all words from the corpus
- c1: number of occurrences of every 1st word in a bigram
- c2: number of occurrences of every 2nd word in a bigram
- c12: number of occurrences of the bigram

## Running the Application

1. Use cmd and navigate to the two projects and run this command for both `mvn compile && package` (make sure ass2-1.0 jar freely in hadoop folder so the pom.xml file will be able to access it)
2. The jar file from "steps" project should be uploaded to your S3 bucket (in my source code it's uploaded hard-coded in HadoopRunner.java in String "bucketName" the bucket `s3://hayon123`).
3. Go to your Home directory (in Windows is usually: C:\Users\<ur username>), create directory ".aws" and create "credentials" file in it and fill with proper AWS credentials
4. Run the jar from the "hadoop" project locally on your PC with the following command:
   `java -jar ass2-1.0.jar ExtractCollations 0.5 0.2` (choose heb or eng hard-coded in HadoopRunner.java file in String "language")
