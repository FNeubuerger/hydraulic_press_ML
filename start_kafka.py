import subprocess

if __name__=="__main__":
    
    kafka_path = '/home/felixneubuerger/kafka_2.12-2.3.0'
    #start zookeeper and kafka server!
    subprocess.call(['sudo', kafka_path+'/bin/kafka-server-start.sh', kafka_path+'/config/server.properties'])
