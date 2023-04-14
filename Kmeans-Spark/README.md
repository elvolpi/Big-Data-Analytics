A spark application in python to implement K-means algorithm to calculate K-means for the device
location.

Data: The file “devicestatus.txt” contains data collected from
mobile devices on Loudacre’s network, including device ID, current status, location and so on. 

Data Scrubbing: Use DeviceStatusETL.py (provided by instructor) 
Script does the following: 
- Load the dataset
- Use the character at position 19 as the delimiter (since the 1st use of the delimiter is at position
19), parse the line, and filter out bad lines. Each line should have exactly 14 fields, so any line
that does not have 14 fields is considered as a bad line.
- Extract the date (first field), manufacturer (second field) (note that the second field contains the
device manufacturer and model name, e.g. Ronin S2, which is separated by a white space),
device ID (third field), and latitude and longitude (13th and 14th fields respectively).
- Save the extracted data to common delimited text files on HDFS.

