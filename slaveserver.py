from flask import Flask, redirect, url_for, request, logging, abort, render_template
import sys
import json
import requests
import threading
import operator
from flask import jsonify

from dhtclasses import startingNode,ChordNode,hashf,getkeyhash
#import redis

app = Flask(__name__)

global ISBOOTSTRAP 
ISBOOTSTRAP = False

if ISBOOTSTRAP:
    global node
    node=startingNode(hardcIp,hardcPort,hardClistofips)

@app.route('/')
def serverHealthCheck():
    return "Server is Gucci"

@app.route('/statuscheck')
def checkme():
    statuslist=[]

    statuslist.append([node.ip,node.port])
    statuslist.append([node.previousIP,node.previousPort])
    statuslist.append([node.nextIP,node.nextPort])
    statuslist.append([node.snip,node.snport])

    return "{},{},{}".format(statuslist,node.my_dict,node.replicadict)

@app.route('/join')
def Join():
    if not ISBOOTSTRAP:
        return "Only Bootstrap nodes handles Join Requests"
    else:
        #an ginoun ola kala
        return node.joinNode()
        
        

@app.route('/createnewnodeHere',methods=['POST'])
def creatnewnode():
    if request.method == 'POST':
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
    
        myip=jsondata["newNodeIp"]
        myPort=jsondata["newNodePort"]
        Sip=jsondata["StartingIp"]
        Sport=jsondata["StartingPort"]
        nIp=jsondata["nIp"]
        nPort=jsondata["nPort"]
        prIp=jsondata["prIp"]
        prPort=jsondata["prPort"]
        k=int(jsondata["repfactor"])
        ct=int(jsondata["constype"])
        #print(k)
        global node
        ISBOOTSTRAP = False
        node=ChordNode(myip,myPort,Sip,Sport,k,ct)
        node.setNextNeighbour(nIp,nPort)
        node.setPrevNeighbour(prIp,prPort)
        
    

    
    #thelw apo ta arguments:1)thn ip tou newnode  2)thn port tou new 3)thn ip tou Stp#stelnw setneighbours!arting 4)to port tou STarting
    #thelw stoixeia gia tous geitones tou !
    #node=ChordNode(...)
    #pou tha prepei na perimenw gia ta data na m ginoun transfer?!
        return "Join Worked!"

@app.route('/setprevn',methods=['POST'])    
def setprevneib():
    if request.method == 'POST' :
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
    
        ip=jsondata["newIp"]
        port=jsondata["newPort"]
        return node.setPrevNeighbour(ip,port)

@app.route('/setnextn',methods=['POST'])
def setnextneib():
    if request.method == 'POST' :
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
    
        ip=jsondata["newIp"]
        port=jsondata["newPort"]
        return node.setNextNeighbour(ip,port)   

@app.route('/transferkeysinNewNode',methods=['POST'])    
def transfermykeys():
    if request.method == 'POST' :
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
    
        ip=jsondata["newIp"]
        port=jsondata["newPort"]
        nodeshash=hashf(ip,port)
        keystotransfer=node.transferkeys(nodeshash,False)
        jsondata = json.dumps(keystotransfer)  #the data is in json format, Python recognizes them as string
        url="http://{}:{}/receivedata".format(ip,port)
        r=requests.post(url,data=jsondata)
        if r.ok:
            return "Succesfull Transfer"
        else:
            return "transferfailed!"
        
    #natasteilw me request sto ip:port



@app.route('/receivedata',methods=['POST'])
def receivedata():
    if request.method == 'POST' :
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
        node.replicadict.update(jsondata)

        if not jsondata:
            return "Done"
        
        else:
            return node.insert_key(jsondata)

    
       

@app.route('/depart')
def departself():
    if ISBOOTSTRAP:
        return  "Bootstrap node cant leave Chord!"
    else:
        node.depart()
    #handle response sta requests kai dwse analogo mhnyma sto cli
        return "Node depart Succesfull!"



@app.route('/deleteme',methods=['POST'])
def deleteme():
    if ISBOOTSTRAP:
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
        ip=jsondata["myip"]
        port=jsondata["myport"]
        node.DeleteNode(ip,port)
        return"Node depart Succesfull!"
    
@app.route('/updateReps',methods=['POST'])
def updatereps():
    if request.method == 'POST':
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
        flag=False
        if jsondata["replicationfactor"]== node.k-1 :
            flag=True
            #print("ginomai clear")
            node.replicadict.clear()

        if jsondata["replicationfactor"]== 0:
            #print(node.my_dict)
            return node.my_dict
        else:
            jsondata["replicationfactor"]-=1
            jsondata=json.dumps(jsondata)
            url="http://{}:{}/updateReps".format(node.previousIP,node.previousPort)
            r=requests.post(url,data=jsondata)
            if r.ok:
                wdict=json.loads(r.text)
                #print(type(wdict))
                #print(wdict)
                if not flag:
                    wdict.update(node.my_dict)
                    return wdict
                else:
                    node.replicadict.update(wdict)
                    return "Success!"
            else:
                return "Something Went Wrong"
    

#0 for Linearizyability-chainrep-Query 1 for Eventual Cons
 
@app.route('/query',methods=['POST'])
def getk():
    if request.method == 'POST':
        if  node.ConsType == 0:
            jsondata =request.get_data()   #now jsondata is in byte format
            jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
            jsondata = json.loads(jsondata)  #now jsondata is in dict format
            key=jsondata["key"]
            if node.amIresponsible(key) :
                if node.k==1:
                    restuple=node.query_key(key)
                    if restuple[0]:
                        return "Key found in the responsible node {}:{}!And value is {}".format(node.ip,node.port,restuple[1])
                    else:
                        return "Key Doesnt Exist!"
                jsondata["repfactor"]=node.k-2
                jsondata=json.dumps(jsondata)
                url="http://{}:{}/queryrep".format(node.nextIP,node.nextPort)
                r=requests.post(url,data=jsondata)
                if r.ok:
                    return r.text
                else:
                    return "failed!"        #proothise to aithma ston next!Prepei na parei apanthsh autos pou esteile to aithma !            
                       
            else:
                dict123=jsondata
                jsondata=json.dumps(dict123)
                url="http://{}:{}/query".format(node.nextIP,node.nextPort)
                r=requests.post(url,data=jsondata)
                if r.ok:
                    return r.text
                else:
                    return "failed!"        #proothise to aithma ston next!Prepei na parei apanthsh autos pou esteile to aithma !
        elif node.ConsType == 1:
            if request.method == 'POST':
                jsondata =request.get_data()   #now jsondata is in byte format
                jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
                jsondata = json.loads(jsondata)  #now jsondata is in dict format
                key=jsondata["key"]
                restuple=node.query_key(key)
                restuple1=node.replica_query(key)

                if node.amIresponsible(key) :
                    if restuple[0]:
                        return "Key found in the responsible node {}:{}!And value is {}".format(node.ip,node.port,restuple[1])
                    else:
                        return "Key Doesnt Exist!"
                
                elif restuple1[0]:
                    return "Key found as replica in node {}:{}!And value is {}".format(node.ip,node.port,restuple1[1])
        
                else:
            
            
                    dict123=jsondata
                    jsondata=json.dumps(dict123)
                    url="http://{}:{}/query".format(node.nextIP,node.nextPort)
                    r=requests.post(url,data=jsondata)
                    if r.ok:
                        return r.text
                    else:
                        return "failed!"        #proothise to aithma ston next!Prepei na parei apanthsh autos pou esteile to aithma !
#Linearizeability-ChainReplication            
@app.route('/queryrep',methods=['POST'])
def queryrep():
    if request.method == 'POST':
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
        key=jsondata["key"]
        repfactor=jsondata["repfactor"]
        
        if repfactor > 0:
            jsondata["repfactor"]=repfactor-1
            jsondata=json.dumps(jsondata)
            url="http://{}:{}/queryrep".format(node.nextIP,node.nextPort)
            r=requests.post(url,data=jsondata)
            if r.ok:
                return r.text
            else:
                return "failed!"
        elif repfactor==0:
            restuple=node.replica_query(key)
            if restuple[0] :
                return "key found as replication in node {}:{}!And value is {}".format(node.ip,node.port,restuple[1])
            else:
                return "key doesnt exist!"
            


##LINEARIZABILITY-CHAINREPLICATION
@app.route('/insert',methods=['POST'])
def insert():
    if request.method == 'POST':
        if node.ConsType == 0:
            jsondata =request.get_data()   #now jsondata is in byte format
            jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
            jsondata = json.loads(jsondata)  #now jsondata is in dict format
            key=jsondata["key"]
            KVdict=dict()
            if node.amIresponsible(key) :
                jsondata["repfactor"]=node.k-2
                KVdict[key]=jsondata["value"]
                node.insert_key(KVdict)
                jsondata=json.dumps(jsondata)
                url="http://{}:{}/insertrep".format(node.nextIP,node.nextPort)
                r=requests.post(url,data=jsondata)
                if r.ok:
                    return r.text
                else:
                    return "failed!"        #proothise to aithma ston next!Prepei na parei apanthsh autos pou esteile to aithma !
                

            else :
                dict123=jsondata
                jsondata=json.dumps(dict123)
                url="http://{}:{}/insert".format(node.nextIP,node.nextPort)
                r=requests.post(url,data=jsondata)
                if r.ok:
                    return r.text
                else:
                    return "failed!"              
        elif node.ConsType == 1:
            jsondata =request.get_data()   #now jsondata is in byte format
            jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
            jsondata = json.loads(jsondata)  #now jsondata is in dict format
            key=jsondata["key"]
            KVdict=dict()
            if node.amIresponsible(key) :
                #jsondata["repfactor"]=node.k-2
                KVdict[key]=jsondata["value"]
                #jsondata=json.dumps(jsondata)
                repfactor=node.k-2
                params={"key":key,"value":jsondata["value"],"repfactor":repfactor}
                url="http://{}:{}/insertrep".format(node.nextIP,node.nextPort)
                t = threading.Thread(target=sendinsertrep,args=(url,params,{}))
                t.daemon = True  # Try commenting this out, running it, and see the difference
                t.start()

                return node.insert_key(KVdict)

                

            else :
                dict123=jsondata
                jsondata=json.dumps(dict123)
                url="http://{}:{}/insert".format(node.nextIP,node.nextPort)
                r=requests.post(url,data=jsondata)
                if r.ok:
                    return r.text
                else:
                    return "failed!"                
            

@app.route('/insertrep',methods=['POST'])
def insertrep():
    if request.method == 'POST':
        if node.ConsType == 0:
            jsondata =request.get_data()   #now jsondata is in byte format
            jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
            jsondata = json.loads(jsondata)  #now jsondata is in dict format
            key=jsondata["key"]
            value=jsondata["value"]
            repfactor=jsondata["repfactor"]
            if repfactor<0:
                return "ok!"
            if repfactor > 0:
                node.replicadict[key]=value
                jsondata["repfactor"]=repfactor-1
                jsondata=json.dumps(jsondata)
                url="http://{}:{}/insertrep".format(node.nextIP,node.nextPort)
                r=requests.post(url,data=jsondata)
                if r.ok:
                    return r.text
                else:
                    return "failed!"
            if repfactor==0:
                dictas=dict()
                dictas[key]=value
                return node.insert_rep(dictas)
        elif node.ConsType == 1:
            key = request.args.get("key")
            value = request.args.get("value")
            repfactor = int(request.args.get("repfactor"))

            dictas=dict()
            dictas[key]=value
            if repfactor<0:
                return "ok!"
            if repfactor > 0:
                node.replicadict[key]=value
                repfactor-=1
                params={"key":key,"value":value,"repfactor":repfactor}
                url="http://{}:{}/insertrep".format(node.nextIP,node.nextPort)
                r=requests.post(url,params=params)
                return node.insert_rep(dictas)
            if repfactor==0:
                return node.insert_rep(dictas)

        
        
@app.route('/delete',methods=['POST'])
def delete():
    if request.method == 'POST':
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
        key=jsondata["key"]
        if node.amIresponsible(key) :
            jsondata["repfactor"]=node.k-2
            jsondata=json.dumps(jsondata)
            url="http://{}:{}/deleterep".format(node.nextIP,node.nextPort)
            r=requests.post(url,data=jsondata)
            if r.ok:
                print("all good")
            else:
                return "failed!"        
            return node.delete_key(key)
            
        else:
            dict123=jsondata
            jsondata=json.dumps(dict123)
            url="http://{}:{}/delete".format(node.nextIP,node.nextPort)
            r=requests.post(url,data=jsondata)
            if r.ok:
                return r.text
            else:
                return "failed!"   

@app.route('/deleterep',methods=['POST'])
def deleterep():
    if request.method == 'POST':
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
        key=jsondata["key"]
        repfactor=jsondata["repfactor"]
        if repfactor > 0:
            jsondata["repfactor"]=repfactor-1
            jsondata=json.dumps(jsondata)
            url="http://{}:{}/deleterep".format(node.nextIP,node.nextPort)
            r=requests.post(url,data=jsondata)
            if r.ok:
                print("Succes!")
            else:
                return "failed!"
        return node.delete_rep(key)


@app.route('/sendall',methods=['POST'])
def sendall():
    if request.method == 'POST':
        if ISBOOTSTRAP:
            jsondata =request.get_data()   #now jsondata is in byte format
            jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
            jsondata = json.loads(jsondata)  #now jsondata is in dict format
            rip=jsondata["ip"]
            rpost=jsondata["port"]
            data=self.connectedNodeData
            jsondata=json.dumps(dict123)
            url="http://{}:{}/receiveall".format(node.rip,node.rip)
            
            r=requests.post(url,data=jsondata)
            if r.ok:
                return "Forwaring to next node.."
            else:
                return "failed!"   
            
        
@app.route('/querystar',methods=['POST'])
def receiveall():
    if request.method == 'POST':        
        jsondata =request.get_data()   #now jsondata is in byte format
        jsondata=jsondata.decode("utf-8")   #now jsondata is in str format
        jsondata = json.loads(jsondata)  #now jsondata is in dict format
        #print(jsondata)
        for key in node.my_dict:
            if key in jsondata.keys():
                return jsondata 
            break
        
        kekdict=dict()
        kekdict.update(jsondata)
        kekdict.update(node.my_dict)
        jsondata=json.dumps(kekdict)
        url="http://{}:{}/querystar".format(node.nextIP,node.nextPort)
        r=requests.post(url,data=jsondata)
        if r.ok:
            return r.text
        else:
            return "failed!"            
    
@app.route('/overlay',methods=['GET'])
def showall():
    
    if not ISBOOTSTRAP:
        url="http://{}:{}/overlay".format(node.snip,node.snport)
            
        r=requests.get(url)
        if r.ok:
            return r.text
        else:
            return "failed!" 
    else:
        nodedict=dict()
        for i in range(len(node.connectednodeData)):
            nodedict[i]=node.connectednodeData[i][2]
        
            
        return nodedict


@app.route('/shutdown',methods=['GET'])
def shutdown():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
        func()

def sendinsertrep(url,params,data):
    
    r=requests.post(url,params=params,data=json.dumps(data))

if __name__ == '__main__':
    ip=str(sys.argv[1])
    porta=int(sys.argv[2])
    app.run(host=ip,port=porta, debug=True,threaded=True)


