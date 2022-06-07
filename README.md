# Find top-n most frequent visitors and urls for each day of the trace from Nasa access file.

## Contributors
Suvro Dey

## Project Requirement
- Downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz and use Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace.
- Develop code in Scala
- FTP Data file location - ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
- GitHub data file location -https://github.com/suvrodey1/frqvisitor/blob/main/NASA_access_log_Jul95
- Pull data from the given FTP location first. Otherwise, from Github.
- Use Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace

## File Detail
### Source Code
- FrqVisitor.scala - Main class
- ReadFromFtpZip.scala - To read the data from ftp folder
- ReadFromGit.scala - To read the data from GitHub
- pom.xml - Used by Maven to build the jar file.

### Test Data file used to test
access_log_Aug95_temp.zip - Unzip and store it in my local machine

### Executable Jar File
target/frqvisit-1.0.0-jar-with-dependencies.jar

## Output Report Detail
- In the local environment the output will be displayed in the console. And the format will be as below -  
**Hostname** : Client Host  
**Url** - URL visited  
**dateAccess** - Date when url accessed.  
**Count** - Number of times the data accessec.  
Below table is showing top 5 records per day i.e. n is 5  
![Output Image](https://github.com/suvrodey1/frqvisitor/tree/main/images))  

## How to Compile and Build Jar file


- 

What the project does
Why the project is useful
How users can get started with the project
Where users can get help with your project
Who maintains and contributes to the project
