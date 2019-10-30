import subprocess

if __name__=="__main__":
    
    kafka_path = '~/kafka_2.12-2.3.0'
    #start Zookeeper and kafka server!
    subprocess.call(kafka_path+'bin/zookeeper-server-start.sh config/zookeeper.properties')
    subprocess.call(kafka_path+'bin/kafka-server-start.sh config/server.properties')