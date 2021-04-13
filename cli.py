
import random
import click
import json
import socket
import requests
from click_help_colors import HelpColorsGroup, HelpColorsCommand
from pyfiglet import Figlet
from termcolor import colored
import time


hardClistofips=[]
hardClistofips.append("192.168.0.1")
hardClistofips.append("192.168.0.2")
hardClistofips.append("192.168.0.3")
hardClistofips.append("192.168.0.4")
hardClistofips.append("192.168.0.5")
ports=[]
ports.append("5000")
ports.append("5001")
filename="RepF10ConsType1"

def choserandomaddress(listip,listport):
    ip=random.choice(listip)
    port=random.choice(listport)
    return [ip,port]

sip ='192.168.0.1'
sport='5000'


@click.group(cls=HelpColorsGroup,help_headers_color='yellow',help_options_color='cyan')
def cli():
    pass
f = Figlet(font='slant')
click.echo(f.renderText('Toy Chord '))
#f = Figlet(font='ntgreek')
#click.secho(colored(f.renderText('BALTE DEKA')),fg='green')
click.echo("\n")




#######################################################################################
@cli.command('healthcheck',short_help='Type healthcheck ip and port  to check the status of a server')
@click.argument('ip',required=True,type=str)
@click.argument('port',required=True,type=int)
def health(ip,port):
    url = "http://{}:{}/".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
    r = requests.get(url)
    if r.ok:
        click.secho('Server responded:',fg='red',bold=True)
        print(r.text)
    else:
        click.secho('Server responded:',fg='red',bold=True)
        print(r.text)
#######################################################################################
@cli.command('statuscheck',short_help='Type statuscheck ip and port  to check the key-values and replicas of a node')
@click.argument('ip',required=True,type=str)
@click.argument('port',required=True,type=int)
def health(ip,port):
    url = "http://{}:{}/statuscheck".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
    r = requests.get(url)
    if r.ok:
        #click.secho('Server responded:',fg='red',bold=True)
        print(r.text)
    else:
        #click.secho('Server responded:',fg='red',bold=True)
        print(r.text)

####################################################################################

@cli.command('join',short_help='Type join and the number of nodes you want to join in ToyChord')
@click.argument('number',required=True,type=int)

def join(number):
    for i in range(number):
        
        url="http://{}:{}/join".format(sip,sport)
    
        r = requests.get(url)  #to send data in json format
        if r.ok:
            #click.secho('Join Request Complete!',fg='red',bold=True)
            print(r.text)
        else:
            #click.secho('something went wrong',fg='red',bold=True)
            print(r.text)
    
###############################################################################

@cli.command('depart',short_help='Type depart and ip , port of a node to depart from toyChord')
@click.argument('ip',required=True,type=str)
@click.argument('port',required=True,type=int)
def join(ip,port):
    #click.secho('The node to depart is {}:{}'.format(ip,port),fg='red',bold=True)
    url="http://{}:{}/depart".format(ip,port)
    
    r = requests.get(url)  #to send data in json format
    if r.ok:
        #click.secho('Depart Request Complete!',fg='red',bold=True)
        print(r.text)
    else:
        click.secho('something went wrong',fg='red',bold=True)

############################################################################


@cli.command('insert',short_help='Type insert and give ip,port of a node and a KEY ,VALUE pair to insert')
@click.argument('ip',required=True,type=str)
@click.argument('port',required=True,type=int)
@click.argument('key',nargs=-1,required=True)
@click.argument('value',required=True)
def insert(ip,port,key,value):
    mydict=dict()
    newkey=' '.join(key)
    #print(newkey)
    mydict["key"]=newkey
    mydict["value"]=value
    mydict["ip"]=ip
    mydict["port"]=port
    #print(key)
    jsondata = json.dumps(mydict)  #the data is in json format, Python recognizes them as string
    url = "http://{}:{}/insert".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
    r = requests.post(url,data=jsondata)
    if r.ok:
        #click.secho('You received the following data',fg='red',bold=True)
        print(r.text)
    else:
        #click.secho('something went wrong',fg='red',bold=True)
        print(r.text)

        
###############################################################################
   

@cli.command('delete',short_help='Type delete and give ip,port of a node  and a KEY to make delete request')
@click.argument('ip',required=True,type=str)
@click.argument('port',required=True,type=int)
@click.argument('key',nargs=-1,required=True)
def delete(ip,port,key):
    mydict=dict()
    newkey=' '.join(key)
    #print(newkey)
    mydict["key"]=newkey
    mydict["value"]=' '
    mydict["ip"]=ip
    mydict["port"]=port
    #print(key)
    jsondata = json.dumps(mydict)  #the data is in json format, Python recognizes them as string
    url = "http://{}:{}/delete".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
    r = requests.post(url,data=jsondata)
    if r.ok:
        #click.secho('You received the following data',fg='red',bold=True)
        print(r.text)
    else:
        #click.secho('something went wrong',fg='red',bold=True)
        print(r.text)

##################################################################################

@cli.command('query',short_help='Type query and give ip,port of a node and a KEY to make query request')
@click.argument('ip',required=True,type=str)
@click.argument('port',required=True,type=int)
@click.argument('key',nargs=-1,required=True)
def query(ip,port,key):
    mydict=dict()
    newkey=' '.join(key)
    mydict["key"]=newkey
    mydict["value"]=' '
    mydict["ip"]=ip
    mydict["port"]=port
    jsondata = json.dumps(mydict)  #the data is in json format, Python recognizes them as string
    url = "http://{}:{}/query".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
    r = requests.post(url,data=jsondata)
    if r.ok:
        #click.secho('You received the following data',fg='red',bold=True)
        print(r.text)
    else:
        #click.secho('something went wrong',fg='red',bold=True)
        print(r.text)  
        
        
#########################################################################################

@cli.command('query*',short_help='Type query"*" (space) ip,port in order to query all the keys in toyChord\n')
@click.argument('ip',required=True,type=str)
@click.argument('port',required=True,type=int)
def queryAll(ip,port):
    mydict=dict()
    #mydict[9]=10
    jsondata = json.dumps(mydict)
    #print(jsondata)
    url = "http://{}:{}/querystar".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
    r = requests.post(url,data =jsondata)  
    if r.ok:
        #click.secho('All the key-value pairs are:',fg='red',bold=True)
        print(r.text)
    else:
        #click.secho('something went wrong re pepega',fg='red',bold=True)
        print(r.text)

#######################################################################################

@cli.command('overlay',short_help='Type overlay ip and porta to Display the topology of toyChord')
@click.argument('ip',required=True,type=str)
@click.argument('port',required=True,type=int)
def overlay(ip,port):
    
    url = "http://{}:{}/overlay".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
    r = requests.get(url)  #to send data in json format
    if r.ok:
        click.secho('Diplaying the toplogy of toyChord',fg='red',bold=True)
        print(r.text)
    else:
        click.secho('something went wrong',fg='red',bold=True)
        print(r.text)  
###############################################################################
@cli.command('insertfile',short_help=' Write insertfile  and the key:value pairs in insert.txt file will be sent')
def insertfile():    
        t0 = time.time()
        test_file = open("insert.txt", "r")
        counter=0
        t0 = time.time()
        for line in test_file:
            [ip,port]=choserandomaddress(hardClistofips,ports)
            test_dictionary={}
            line=line.strip('\n')
            (key,value)=line.split(",")
            test_dictionary["key"]=key
            test_dictionary["value"]=value
            test_file=json.dumps(test_dictionary) #the data is in json format, Python recognizes them as string
            url = "http://{}:{}/insert".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
            r = requests.post(url,data =  test_file)  #to send data in json format
            if r.ok:
                print(r.text)
                f=open("{}_insert.txt".format(filename),"a+")
                f.write(r.text)
                f.write("\n")
                f.close()

            else:
                click.secho('something went wrong',fg='red',bold=True)
            counter+=1
        t1 = time.time()
        total = t1-t0
        timelapse='{} Insert Requests Handled in {} seconds'.format(counter,total)
        click.secho(timelapse,fg='yellow',bold=True)
            
        f=open("{}_insert.txt".format(filename),"a+")
        f.write(timelapse)
        f.write("\n")
        f.close()
####################################################################################

@cli.command('queryarxeio',short_help='Write queryarxeio in order to query all the keys in query.txt file')
def queryarxeio():
    [ip,port]=choserandomaddress(hardClistofips,ports)
    test_file = open("query.txt", "r")
    lines=test_file.readlines()
    counter=0
    t0 = time.time()
    for i in lines:
        [ip,port]=choserandomaddress(hardClistofips,ports)
        i=i.strip('\n')
        mydict=dict()
        mydict["key"]=i
        jsondata = json.dumps(mydict)
        url = "http://{}:{}/query".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
        r=requests.post(url,data =jsondata)  #to send data in json format            
        if r.ok:
            f= open("{}_queryfile.txt".format(filename),"a+")
            f.write(r.text)
            f.write("\n")
            f.close()
            print(r.text)
            counter+=1
        else:
            f= open("{}_queryfile.txt".format(filename),"a+")
            f.write(r.text)
            f.write("\n")
            f.close()


    t1 = time.time()
    total = t1-t0
    timelapse='{} Query Requests Handled in {} seconds'.format(counter,total)
    click.secho(timelapse,fg='yellow',bold=True)
    f=open("{}_queryfile.txt".format(filename),"a+")
    f.write(timelapse)
    f.write("\n")
    f.close()

        
########################################################################################


@cli.command('request',short_help='Write request and the content of requests.txt will be executed\n')
def request(): 
    test_file = open("requests.txt", "r")
    lines=test_file.readlines()
    t0 = time.time()
    counti=0
    countq=0
    for i in lines:
        [ip,port]=choserandomaddress(hardClistofips,ports)
        firstword=i.split()[0]
        firstword=firstword[:-1]
        #print(firstword)
        counter=1
        if firstword=='insert':
            counti+=1
            first, *middle, last = i.split()
            middle = ' '.join(map(str, middle))
            middle=middle[:-1]
            #now first=h prwth leksh ths grammhs,middle=key ths grammhs,last=value ths grammhs OLA EINAI STR          
            mydict=dict()
            newkey=''.join(middle)
            #print(middle)
            mydict["key"]=newkey
            mydict["value"]=last
            #print(mydict[middle])
            #print(type(mydict))
            jsondata = json.dumps(mydict)  #the data is in json format, Python recognizes them as string
            #print(jsondata)
            #print(type(jsondata))
            url = "http://{}:{}/insert".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
            r=requests.post(url,data =jsondata)  #to send data in json format            
            if r.ok:
                #click.secho('You inserted the following data succesfully',fg='red',bold=True)
                print(r.text)
                f=open("{}_request.txt".format(filename),"a+")
                f.write(r.text)
                f.write("\n")
                f.close()
            else:
                click.secho('something went wrong in insertion',fg='red',bold=True)
                
        else:
            countq+=1
            first, *middle = i.split()
            middle = ' '.join([str(elem) for elem in middle])
            #print(middle)
            #print(type(middle))
            mydict=dict()
            newkey=''.join(middle)
            #print(middle)
            mydict["key"]=newkey
            jsondata = json.dumps(mydict)  #the data is in json format, Python recognizes them as string
            url = "http://{}:{}/query".format(ip,port)   #ftiaxnw to url analoga to endpoint pou tha orisoume sto server 
            r = requests.post(url,data=jsondata)
            if r.ok:
                print(r.text)
                f=open("{}_request.txt".format(filename),"a+")
                f.write(r.text)
                f.write("\n")
                f.close()

                #click.secho('The query was succesfull',fg='red',bold=True)
                print(r.text)

            else:
                click.secho('something went wrong at the query',fg='red',bold=True)
    t1 = time.time()
    total = t1-t0
    timelapse='{} Insert and {} Query Requests Handled in {} seconds'.format(counti,countq,total)
    click.secho(timelapse,fg='yellow',bold=True)
    f=open("{}_request.txt".format(filename),"a+")
    f.write(timelapse)
    f.write("\n")
    f.close()




if __name__ == '__main__':
    cli()

