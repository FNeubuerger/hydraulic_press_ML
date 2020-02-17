# hydraulic_press_ML
This is a repository for the analysis of the publically availiable Hydraulic Press Data Set for condition monitoring availiable at: https://archive.ics.uci.edu/ml/datasets/Condition+monitoring+of+hydraulic+systems#

# Installation
Download Apache kafka and install it as described on: https://kafka.apache.org/quickstart (use Kafka >=2.12-2.4.0)

# Usage:
### Linux:
Start Zookeeper and Kafka with the corresponding python scripts. (Be sure to update paths if not default installation in home/kafka_version)

Run the Producer to stream Data from a csv file

Run the Consumer to handle incoming data and do processing on it.

### Windows:
Install Kafka as described and change paths/commands in subprocess calls in the helper scripts.

NOT TESTED

# Resources

Some Jupyter Notebooks für Data Preparation and Model Prototyping with Tensorflow

Helper scripts for Kafka Streaming


# Feel free to contribute a Docker containerization for this whole thing