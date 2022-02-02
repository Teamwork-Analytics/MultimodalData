import paho.mqtt.client as mqtt
import ssl
import datetime
import keyboard
from mysql.connector import MySQLConnection, Error
from configparser import ConfigParser
import json
from datetime import datetime

saveInFile = 1
saveInDatabase = 0
show_input= 1
host = "localhost" #"172.19.63.52"
port = 1883
topic = "tagsLive"
idSession =datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
if saveInFile==1:
    file = open(idSession+".json","w") 

def read_db_config(filename='config.ini', section='mysql'):
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """
    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)
 
    # get section, default to mysql
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))
 
    return db

def insert_tag_location(idTag,x,y,z,timePoint,idSession,accX,accY,accZ,Pitch,Yaw,Roll):
    query = "INSERT INTO raw(id_tag,x,y,z,timePoint,session,acc_x,acc_y,acc_z,yaw,pitch,roll) " \
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    args = (idTag,x,y,z,timePoint,idSession,accX,accY,accZ,Pitch,Yaw,Roll)
 
    try:
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)
 
        cursor = conn.cursor()
        cursor.execute(query, args)
 
        #if cursor.lastrowid:
        #    print('last insert id', cursor.lastrowid)
        #else:
        #    print('last insert id not found')
 
        conn.commit()
    except Error as error:
        print(error)
 
    finally:
        cursor.close()
        conn.close()
        return cursor.lastrowid

def insertToDatabaseFromJSON(line):
    data = json.loads(line)
    if data['success'] == True:
        idTag = data['tagId']
        x = data['data']['coordinates']['x']
        y = data['data']['coordinates']['y']
        z = data['data']['coordinates']['z']
        timePoint = datetime.fromtimestamp(data['timestamp']).strftime('%Y-%m-%d %H-%M-%S')
        accX = data['data']['acceleration']['x']
        accY = data['data']['acceleration']['y']
        accZ = data['data']['acceleration']['z']
        Pitch = data['data']['orientation']['pitch']
        Yaw = data['data']['orientation']['yaw']
        Roll = data['data']['orientation']['roll']
        res = insert_tag_location(idTag,x,y,z,timePoint,idSession,accX,accY,accZ,Pitch,Yaw,Roll)

def insertToDatabasePlainJSON(line):
    query = "INSERT INTO json(session,json) " \
            "VALUES(%s,%s)"
    args = (idSession,line)
 
    try:
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)
 
        cursor = conn.cursor()
        cursor.execute(query, args)
 
        if cursor.lastrowid:
            print('last insert id', cursor.lastrowid)
        else:
            print('last insert id not found')
 
        conn.commit()
    except Error as error:
        print(error)
 
    finally:
        cursor.close()
        conn.close()
        return cursor.lastrowid

def on_connect(client, userdata, flags, rc):
    print(mqtt.connack_string(rc))
# callback triggered by a new Pozyx data packet
def on_message(client, userdata, msg):
    if show_input == 1:
        print("Positioning update:", msg.payload.decode())
    if saveInFile==1:
        file.write(msg.payload.decode()+"\n")
    if saveInDatabase==1:
        insertToDatabaseFromJSON(msg.payload.decode())
   
    
def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed to topic!")
def on_disconnect(client, userdata, flags, rc):
    print("Try to connect here")
    if saveInFile==1:
        file.close()
    client.connect(host, port=port)
    client.subscribe(topic)
    # works blocking, other, non-blocking, clients are available too.
    client.loop_forever()
    
# callback triggered by a new Pozyx data packet
client = mqtt.Client()
# set callbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_subscribe = on_subscribe
client.connect(host, port=port)
client.subscribe(topic)
# works blocking, other, non-blocking, clients are available too.
client.loop_forever()

while True:
    if keyboard.is_pressed('Q'):
        print("Q pressed")
        client.unsuscribe(topic)
        if saveInFile==1:
            file.close()       
