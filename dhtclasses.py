
from flask import Flask,request
import logging
import hashlib 
import operator
import sys
import json
import requests
import operator
import time

global ISDEPART
ISDEPART=False

class ChordNode(object):

        my_dict=dict()
        
        def __init__(self,ip,port,SNIp,SNPort,k,ct):
                self.ip=ip
                self.port=port
                
                self.id=hashf(self.ip,self.port)
                self.previousIP=self.ip
                self.previousPort=self.port
                self.nextIP=self.ip
                self.nextPort=self.port
                self.snip=SNIp
                self.snport=SNPort
                self.k=k
                self.replicadict=dict()
                self.ConsType=ct#0 for linear 1 for eventual
                
        def setNextNeighbour(self,nextNeibIp,nextNeibPort):
                self.nextIP=nextNeibIp
                self.nextPort=nextNeibPort
                return "Succesfull Neighbour change"
        
        def setPrevNeighbour(self,prevNeibIp,prevNeibPort):
                self.previousIP=prevNeibIp
                self.previousPort=prevNeibPort
                return "Succesfull Neighbour change"
                
        def amIresponsible(self,key):
                hashkey=getkeyhash(key)
                prevhashkey=hashf(self.previousIP,self.previousPort)

                if prevhashkey>self.id :
                        return self.id>hashkey or hashkey>prevhashkey
                #an eimai mikroteros apo ton prohgoumeno mou  tote to kleidi mou anhkei se periptwsh pou einai megalutero kai apo tous duo mas ,h mikrotero kai apo tous duo mas 
                elif prevhashkey<self.id:
                        return ((prevhashkey<hashkey)and(hashkey<self.id))
                #an eimai megaluteros apo ton prohgoumeno mou totte to kleidi mou nahkei an einai anamesa mas..
                else:
                        return hashkey!=prevhashkey and hashkey!=self.id
                #den paizei auto
        
        def transferkeys(self,hash,ISDEPART):
                newdict=dict()
                if ISDEPART:
                        #newdict=self.my_dict
                        for key in self.my_dict:
                                newdict[key]=self.my_dict[key]
                else:
                        for key in self.my_dict:
                                if not self.amIresponsible(key):
                                        newdict[key]=self.my_dict[key]
                
                for key in newdict:
                        del self.my_dict[key]
                        
                return newdict
                                
                       
        def depart(self):
                sendingdict=self.transferkeys(self.id,True)
                jsondata = json.dumps(sendingdict)  
                url="http://{}:{}/receivedata".format(self.nextIP,self.nextPort)
                
                with requests.post(url,data=jsondata) as r:
                
                        if r.ok:
                                asdf=1#print("My data are sent succesfully!")                
                        else:
                                return "Error while sending data"
                                

                datadict={"IncomingIp":self.ip,"IncPort":self.port,"newIp":self.nextIP,"newPort":self.nextPort,"BSnodeip":self.snip,"Bsnodeport":self.port,"repfactor":self.k}
                jsondata=json.dumps(datadict)
                url="http://{}:{}/setnextn".format(self.previousIP,self.previousPort)
                                
                with requests.post(url,data=jsondata) as r:
                
                        if r.ok:
                                asdsf=1#print("SetNext Succesfully")                
                        else:
                                return "Error while Setnext"
                
                
                datadict={"IncomingIp":self.ip,"IncPort":self.port,"newIp":self.previousIP,"newPort":self.previousPort,"BSnodeip":self.snip,"Bsnodeport":self.port}
                jsondata=json.dumps(datadict)

                url="http://{}:{}/setprevn".format(self.nextIP,self.nextPort)
                with requests.post(url,data=jsondata) as r:
                
                        if r.ok:
                                asdf=1#print("SetPrev Succesfully")                
                        else:
                                return "Error while SetPrev"                
                

                datadict={"myip":self.ip,"myport":self.port}
                jsondata=json.dumps(datadict)
                
                url="http://{}:{}/deleteme".format(self.snip,self.snport)
                r=requests.post(url,data=jsondata)
                
                if r.ok:
                        asdf=1#print("DeleteMe request Succesfull")                
                else:
                        return "Error while Deleteme"                  
                
                
        def mydata(self):
                return self.my_dict
                
        def query_key(self,key):
                if self.my_dict.get(key) == None :
                        return [False,"none"]
                else:
                        return [True,self.my_dict.get(key)]
        def replica_query(self,key):
                if self.replicadict.get(key) == None :
                        return [False,"none"]
                else:
                        return [True,self.replicadict.get(key)]
                

        def insert_key(self,Dict):#insert or update to value
                
                for key in Dict:
                        if key in self.my_dict:
                                self.my_dict.update(Dict)
                                return "Succesfully updated Value!"
                        else:
                                self.my_dict.update(Dict)
                                return "Succesfully inserted Value!"
                                
             
                
        
        def insert_rep(self,Dict):#insert or update to value
                
                for key in Dict:
                        if key in self.replicadict:
                                self.replicadict.update(Dict)
                                return "Succesfully updated Value!"
                        else:
                                self.replicadict.update(Dict)
                                return "Succesfully inserted Value!"
                
               
        def delete_key(self,key):
                if self.my_dict.get(key) == None :
                        return "Key doesnt exist!"
                else:                                
                        del self.my_dict[key]
                        return "Key Deleted Succesfully!"
        def delete_rep(self,key):
                if self.replicadict.get(key) == None :
                        return "Key doesnt exist!"
                else:                                
                        del self.replicadict[key]
                        return "Key Deleted Succesfully!"

        def allinn(self):
                return  self.my_dict

        
def getkeyhash(key):
        hash_object=hashlib.sha1(str.encode(key))
        return hash_object.hexdigest()

def hashf(ip,port):
        hash_object=hashlib.sha1(str.encode("{}:{}".format(ip,port)))
        return hash_object.hexdigest()

class startingNode(ChordNode):
         
#einai kai node
        def __init__(self,ip,port,SystemIPlist,k,ct):
                super().__init__(ip,port,ip,port,k,ct)
                self.ipsandnodes=[]
                for i in range(len(SystemIPlist)):
                        if SystemIPlist[i] == self.ip:
                                self.ipsandnodes.append([self.ip,1])
                                
                        else:
                                self.ipsandnodes.append([SystemIPlist[i],0])
                
                
                self.my_dict=randomdatagenerator()
                self.replicadict=randomdatagenerator()        
                self.numberofnodes=1#me ton starting node dld emena 
                self.connectednodeData=[[self.ip,self.port,hashf(ip,port)]] #ip,port kai hash twn sundedemenwn komvon 
#?!#            
                
        def joinNode(self):
                self.numberofnodes+=1
                
                self.ipsandnodes.sort(key=operator.itemgetter(1))

                [newNodeIp,nodesinthisIP]=self.ipsandnodes[0]
                for i in range(20):
                        if [newNodeIp,"{}".format(i+5000),hashf(newNodeIp,"{}".format(5000+i))] not in self.connectednodeData:
                                newNodePort="{}".format(i+5000)
                                break
                
                self.ipsandnodes[0][1]+=1
                self.connectednodeData.append([newNodeIp,newNodePort,hashf(newNodeIp,newNodePort)])
                self.connectednodeData.sort(key=operator.itemgetter(2))

                
                newnodeIndex=self.connectednodeData.index([newNodeIp,newNodePort,hashf(newNodeIp,newNodePort)])
                
                prevNeigIndex= (newnodeIndex-1)%(self.numberofnodes)
                nextNeigIndex= (newnodeIndex+1)%(self.numberofnodes)

                prIp=self.connectednodeData[prevNeigIndex][0]
                prPort=self.connectednodeData[prevNeigIndex][1]
                nextIp=self.connectednodeData[nextNeigIndex][0]
                nextPort=self.connectednodeData[nextNeigIndex][1]
                
                datadict=dict()
                datadict={"newNodeIp":newNodeIp,"newNodePort":newNodePort,"StartingIp":self.ip,"StartingPort":self.port,"prIp":prIp,"prPort":prPort,"nIp":nextIp,"nPort":nextPort,"repfactor":self.k,"constype":self.ConsType}
                
                jsondata = json.dumps(datadict)  #the data is in json format, Python recognizes them as string
                
                url="http://{}:{}/createnewnodeHere".format(newNodeIp,newNodePort)
                r=requests.post(url,data=jsondata)
                if r.ok:
                        asdf=1#print("Node Created Succesfully")                
                else:
                        return "Error while creating node"                
                
                datadict={"newIp":newNodeIp,"newPort":newNodePort,"StartingIp":self.ip,"StartingPort":self.port}
                
                jsondata = json.dumps(datadict)  #the data is in json format, Python recognizes them as string                
                
                url="http://{}:{}/setprevn".format(nextIp,nextPort)
                r=requests.post(url,data=jsondata)
                
                if r.ok:
                        asdf=1#print("SetPrev Succesfully")                
                else:
                        return "Error while SetPrev"                
                
                url="http://{}:{}/setnextn".format(prIp,prPort)
                
                r=requests.post(url,data=jsondata)
                
                if r.ok:
                        asdf=1#print("SetNext Succesfully")                
                else:
                        return "Error while Setnext"
                
                
                if(nextIp==self.ip and nextPort==self.port):
                        
                        keystotransfer=self.transferkeys(self.id,False)
                        jsondata=json.dumps(keystotransfer)
                        url="http://{}:{}/receivedata".format(newNodeIp,newNodePort)
                        r=requests.post(url,data=jsondata)
                        if r.ok:
                                asdf=1#print ("Succesfull Transfer")
                        else:
                                return "transferfailed!"
                else:
                        
                
                        url="http://{}:{}/transferkeysinNewNode".format(nextIp,nextPort)
                        r=requests.post(url,data=jsondata)
                        #edw prepei na tou steile kai ola ta replicas pou exei !
                
                        if r.ok:
                                asdf=1#print("Transfered Succesfully")                
                        else:
                                return "Error while Transfer"
                repsupdated=0
                if(self.k>1 and self.numberofnodes==self.k):
                        for i in range(len(self.connectednodeData)):
                                Ip=self.connectednodeData[i][0]
                                Port=self.connectednodeData[i][1]
                                datadict={"replicationfactor":self.k-1}
                                jsondata=json.dumps(datadict)
                                url="http://{}:{}/updateReps".format(Ip,Port)
                                r=requests.post(url,data=jsondata)
                                if r.ok:
                                        repsupdated+=1
                                else:
                                        return "Something went wrong"

                                
                if(self.k>1 and self.numberofnodes>self.k):

                        for i in range(self.k+1):
                                Ip=self.connectednodeData[(newnodeIndex+i)%self.numberofnodes][0]
                                Port=self.connectednodeData[(newnodeIndex+i)%self.numberofnodes][1]
                                datadict={"replicationfactor":self.k-1}
                                jsondata=json.dumps(datadict)
                                url="http://{}:{}/updateReps".format(Ip,Port)
                                r=requests.post(url,data=jsondata)
                                if r.ok:
                                        repsupdated+=1
                                else:
                                        return "Something went wrong"
                           
                                        
                                
                        
                
                return "Succesfull Join"
                
                
         
        def DeleteNode(self,deletingNodeIp,deletingNodePort):
                if [deletingNodeIp,deletingNodePort,hashf(deletingNodeIp,deletingNodePort)] not in self.connectednodeData :
                        return "ip,port doesnt exist in the snodes data?!"
                else:
                        
                        self.connectednodeData.sort(key=operator.itemgetter(2))
                        index=self.connectednodeData.index([deletingNodeIp,deletingNodePort,hashf(deletingNodeIp,deletingNodePort)])
                        self.connectednodeData.remove([deletingNodeIp,deletingNodePort,hashf(deletingNodeIp,deletingNodePort)])
                        self.connectednodeData.sort(key=operator.itemgetter(2))
                        self.numberofnodes-=1
                        for i in range(len(self.ipsandnodes)):
                                if self.ipsandnodes[i][0] == deletingNodeIp:
                                        self.ipsandnodes[i][1]-=1
                                        break
                        
                        if(self.k>1 and self.numberofnodes==self.k):
                                repsupdated=0
                                for i in range(len(self.connectednodeData)):
                                        Ip=self.connectednodeData[i][0]
                                        Port=self.connectednodeData[i][1]
                                        datadict={"replicationfactor":self.k-1}
                                        jsondata=json.dumps(datadict)
                                        url="http://{}:{}/updateReps".format(Ip,Port)
                                        r=requests.post(url,data=jsondata)
                                        if r.ok:
                                                repsupdated+=1
                                        else:
                                                return "Something went wrong"
                                if repsupdated == self.k :
                                        return "Replicas Updated!Join Succes!"
                        
                        #einai san arxikooihsh ths leitourgias ton replica!                        
                        #o komvos pou "bainei sth thesh tou komvou pou kanei depart" exei swsta pragmata mesa omws ananewthike to dict tou ..ara prepei oi k-1 epomenoi na ananewsoun ta reps tous!
                        if(self.k>1 and self.numberofnodes>self.k):
                                repsupdated=0
                                for i in range(self.k):
                                        Ip=self.connectednodeData[(index+i)%self.numberofnodes][0]
                                        Port=self.connectednodeData[(index+i)%self.numberofnodes][1]
                                        datadict={"replicationfactor":self.k-1}
                                        jsondata=json.dumps(datadict)
                                        url="http://{}:{}/updateReps".format(Ip,Port)
                                        r=requests.post(url,data=jsondata)
                                        if r.ok:
                                                repsupdated+=1
                                        else:
                                                return "Something went wrong"
                                if repsupdated == self.k-1 :
                                        return "Replicas Updated!Join Succes!"
                        #return kapoio message sto cli pou ekane depart?!!!!


def randomdatagenerator():
        dicta=dict()
        
        #for i in range(0):
        #        a=('{}'.format(i))
        #        dicta[a]=7*i
        return dicta
