# kerry-stedi-analytics
This repo contains works to build a data lakehouse solution for sensor data that trains a machine learning model.

# Data Sources
- Customer data comes from fulfillment and the STEDI website and are saved as JSON formats in S3 bucket.
- Accelerometer data contains records from the mobile app
- Step trainer records are collected from the IOT motion sensor

# Contents at A Glance
This datalake contains scripts that extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Landing Zone
Data comes from the sources are loaded to the landing zone without any manipulation or cleanups.
- customer_landing
- accelerometer_landing
- step_trainer_landing

## Trusted Zone and Research Consents

The trusted zone contains customer, accelerometer reading, and step trainer data only on customers that have consented us to use their data for research purpose.

- customer_trusted: only customer data for those who gives us permission to share data for research purpose
- accelerometer_trusted: only contain mobile app readings for customers who give us the research consent
- step_trainer_trusted: only contains motion sensor data for customer who have accelerometer data and who give us research consent

## Curated Zone
Data in this zone are further cleaned up with corresponding accelerometer and step trainer data and prepared further for machine learning projects.