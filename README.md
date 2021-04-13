# Distributed-Ntua
A simple file transfer app using a DHT based on the Chord protocol 








Make sure you have python3.5 installed 
Set up the virtual environment for the Project:

$ mkdir project && cd project

$ git init

$ python3 -m venv env

$ source env/bin/activate

$ touch .gitignore README.md requirements.txt

This will give you the following structure:
├── .gitignore

├── README.md

└── requirements.txt

Next install Flask:
$ python -m pip install Flask==1.1.1
$ python -m pip freeze > requirements.txt

Install some dependencies and all the dependencies that the execution of the files will ask:
e.g.
$ pip install requests
$ pip install termcolor
$ pip install pyfiglet

etc...
#To run bootstrap node (make sure you are in a virtual env (run the command source env/bin/activate)) 

├── server.py

├── dhtclasses.py

Run the server: 
python3 server.py X Y
X=Replication Factor for Chord system (e.g 1,3,5)
Y=Consistency Type(0 for Linearizeability w/ chain rep,1 for Eventual Consistency)


#To run a slaveserver for a node(make sure you are in a virtual env): 
├── slaveserver.py

├── dhtclasses.py

└── script.sh
run script.sh IP,PORT
This creates a copy file of slaveserver (slaveserver_IP_port.py)
and runs the slaveserver in this ip and port.

To run a command from the cli put cli.py in a virtual env and run python3 cli.py --help and see the commands.  (files insert.txt,requests.txt,query.txt related to some of the commands of cli should be in the same directory with cli.py)
Warning:The system requires to hardcode the possible ip’s and port’s in the cli.py,server.py,slaveserver.py.
