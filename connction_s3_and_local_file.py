#python  and  s3
import boto3
import configparser
from os import listdir
from os.path import isfile, join

config = configparser.ConfigParser()
config.read('D:\\Documents\\GitHub\\Python\\config.ini')
aws_key = config['DEFAULT']['aws_access_key_id']
aws_secret_key = config['DEFAULT']['aws_secret_access_key']
region = config['DEFAULT']['region']
local_file = config['DEFAULT']['local_file']
BUCKET = config['DEFAULT']['bucket']


buck = []
ruta = []
delet = []
file = ""
comparacion = []

#coneection by client
session = boto3.Session(profile_name='default')
s3Client = session.client('s3',region_name=region)
s3Resource = session.resource('s3', region_name=region)   

#connect bucket s3
Bucket =s3Resource.Bucket(BUCKET)
for obj in Bucket.objects.all():
    buck.append(obj.key)
    print (buck)
    
#connect local folder
ruta = ([arch for arch in listdir(local_file) if isfile(join(local_file, arch))])

#save file local in a list
for  item in ruta:
    if not item in buck:
        comparacion.append(item)

#load folders
for file in comparacion:
    rutas = local_file + "\\" + file
    def upload_file():
        s3Resource.Bucket(BUCKET).upload_file(rutas, file)
        print ("creating files")

    upload_file()
    
#delete files
#see deleted files in the local folder

for  item in buck:
    if not item in ruta:
        delet.append(item)

#delete files
for file in delet:
    def delete_object():
        s3Client.delete_object(Bucket=BUCKET, Key=file)
        print ("deleting files")
    delete_object()

#update files
for file in ruta:
    rutas = local_file + "\\" + file
    def update_file():
        s3Resource.Bucket(BUCKET).upload_file(rutas, file)
        print ("update files")

    update_file()
