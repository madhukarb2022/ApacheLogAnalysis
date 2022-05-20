# ApacheLogAnalysis
Spark Scala project for Apache Access log analysis


### My understanding of the requirement
1. Read the Apache Access Log as source data from an FTP location
   - If the FTP is not accessible use a GIT hub location as secondary source
2. Parse the file and extract key fields like SourceIP/Hostname, request date, end point (requested resource), HTTP response code
3. Group the data by request date
4. Find the topN hostname/sourceIP for each date
5. Find the topN endPoint urls for each date
