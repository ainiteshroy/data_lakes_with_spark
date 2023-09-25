## Overview
In this project we attempted to create a datalake for Sparkify. Song-logs and user-logs data stored in S3 was converted to facts and dimensional tables and then stored back onto the S3.

## How to use
1. Create an S3 bucket.
2. Run the etl.py script to have all the source data logs converted and stored on the S3.

> **_Project not running as of now_**
Still unable to read data from S3. Everything else looks fine however. etl.py runs okay with local data. Web search seem to suggest that the issue could be with jar or sdk versions.
I have left the notebook in the submission to indicate the error.