# FinalGroupProject-Data

Zip-potify is an amazing new music streaming company with listeners all over the country.

We, the mgmt team, want you, the data engineering team, to create a cool new dashboard that takes in historical listening data and show us cool things. What cool things?

Regional differences, popularities, and other metrics; show it to us by? artist? song? genre? time? what else?

## Pipeline for Data Processing of a Music Streaming Company

The project will stream events that are created with EventSim. 
Your instructor will give you the link to it.
There is a lot of it, and you'll have to use some "head" and "tail" samples to worj with until you have the kinks worked out.
(You'll use some small subset of the data to get things going, and then turn our attention to all of it.)

You will clean the data, convert the data, and aggregate the data using data engineering techniques.
Clean up and aggregation can be done with various tech you have learned. 
The processed data are saved in a database (MySQL?).

Then make use of this data by consuming it, applying transformations to it, and creating the tables that are needed for our **dashboard** so that analytics may be generated. 
We are going to try to conduct an analysis of indicators such as the most played songs, active users, user demographics, regional differences etc.

Apache Kafka and Apache Spark are two examples of streaming technologies that are used for processing data in (somewhat) real-time. 
The processed data are uploaded to a database, where they are then subjected to transformation. 
We can clean the data, convert the data, and aggregate the data using your tools so that it is ready for analysis. 
The data is then sent to a data warehouse, and tools are used to create a visual representation of the data. 
Apache AirFlow has been used for the purpose of orchestration, whilst Docker is the tool of choice when it comes to containerization.

[eventsim](https://github.com/ZipCodeCore/eventsim) is where the data comes from, but don't worry, you won't have to generate the data yourselves.
