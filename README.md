# Collocation-Extraction

## Abstract

This project focuses on automatically extracting collocations from the Google 2-grams dataset using Amazon Elastic Map Reduce. Collocations are essential in natural language processing and information extraction applications. We will use Normalized Pointwise Mutual Information (NPMI) to identify collocations.

## Collocations

A collocation is a sequence of words that co-occur more often than expected by chance. For example, "crystal clear" and "cosmetic surgery" are collocations. In this project, we will use NPMI to determine if a pair of ordered words is a collocation. 

## Normalized PMI

Pointwise Mutual Information (PMI) is a measure of association between two words. Normalized PMI is calculated based on the count of a word pair, the count of each individual word, and the total number of bigrams in the corpus.

## The Project Goal

A map-reduce job for collocation extraction for each decade using Amazon Elastic MapReduce. The criteria for collocations are based on the normalized PMI value. The input is the Hebrew and English 2-Gram datasets of Google Books Ngrams.

## Stop Words

Stop words are words that appear frequently in the corpus and are often filtered out. In this project, we remove all bigrams that contain stop words from the counts.


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
