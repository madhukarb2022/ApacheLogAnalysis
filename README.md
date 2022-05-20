# ApacheLogAnalysis
Spark Scala project for Apache Access log analysis

### My understanding of the requirement
1. Read the Apache Access Log as source data from an FTP location
   - If the FTP is not accessible use a GIT hub location as secondary source
2. Parse the file and extract key fields like SourceIP/Hostname, request date, end point (requested resource), HTTP response code
3. Group the data by request date
4. Find the topN hostname/sourceIP for each date
5. Find the topN endPoint urls for each date

### What I did
1. Created a Maven Scala project
2. Analyzed the source data downloaded from FTP location
3. Created a RegEx parser that handles different types of requests and Unit test for each case
4. Tried 2 approaches in Spark to group the data for each date
5. Verified that both approaches return the same result
6. Wrote generic code to find topN by both group-by fields (hostname/IPAddress & endPoint/requetsed url)
7. Created a Docker image using jib-maven-plugin
8. Downloaded the Docker image to my local machine and verified with a run

### Docker Image
1. Docker Image is present in the folder "/Docker"
2. Load the Image using
   - docker load --input jib-image.tar
3. Run the program using
   - docker run access-analysis-image

### What can be done better (if I had more time)
1. Write Unit tests for all the methods of AccessLogAnalysisMain
2. Analyze the df.explain() output to improve the performance
3. All parameters like "n", source primary/secondary urls are in the code. They can be externalized to be passed through Docker
