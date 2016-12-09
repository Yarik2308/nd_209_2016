import urllib
import sys
import os, zipfile
import glob

import json
import sqlite3
import io


######## Load and unZip
if glob.glob('*').count('vote-master')==0:
    if glob.glob('*').count('vote-master.zip')==0:
        url = 'https://codeload.github.com/data-dumaGovRu/vote/zip/master'
        urllib.urlretrieve(url, 'vote-master.zip')
        
    zi = zipfile.ZipFile('vote-master.zip', 'r',encoding = 'utf-8-sig')
    zi.extractall()
    zi.close()



try:
    open(sys.path[0] + '/main.db', 'r')
except IOError as e:
    ######## SQL
    conn = sqlite3.connect(sys.path[0]+'/main.db')
    c = conn.cursor()
    ########Votes
    c.execute('CREATE TABLE IF NOT EXISTS VoteID(VotID INT UNIQUE NOT NULL, Title VARCHAR(500), Date VARCHAR(25), Url VARCHAR(60), PRIMARY KEY(VotID) )')

    c.execute('CREATE TABLE IF NOT EXISTS FactionID(FacID INT UNIQUE NOT NULL, Name VARCHARM(10), PRIMARY KEY(FacID))')

    c.execute('CREATE TABLE IF NOT EXISTS Votes(VoteID INT NOT NULL, FacID INT NOT NULL, Accept INT, None INT, Decline INT, Abstain INT, FOREIGN KEY(VoteID) REFERENCES VoteID(VotID),'
    'FOREIGN KEY(FacID) REFERENCES FactionID(FacID) )')

    c.execute('CREATE TABLE IF NOT EXISTS VoteDep(VoteID INT NOT NULL,DepId INT NOT NULL,Result VARCHAR(8), FOREIGN KEY(VoteID) REFERENCES VoteID(VotID),'
              'FOREIGN KEY(DepID) REFERENCES DeputysID(DepID) )')

    ###### Deputy
    c.execute('CREATE TABLE IF NOT EXISTS DeputysID(DepID INT UNIQUE NOT NULL, Name VARCHAR(50), PRIMARY KEY(DepID))')
    c.execute('CREATE TABLE IF NOT EXISTS Deputys(DepId INT, FacId INT, Date VARCHAR(25), DateLast VARCHAR(25), FOREIGN KEY(DepID) REFERENCES DeputysID(DepID),'
              'FOREIGN KEY(FacID) REFERENCES FactionID(FacID))')
    
    os.chdir(sys.path[0]+'/vote-master/poll')
    votes = {}
    id = {}
    DataAll = {}
    DataN = {}
    Data = {}
    lastdate = '0'
    b = 1

    for folder in glob.glob('*'):
        for file in glob.glob(folder + '/*'):
            with io.open(file, "r", encoding="utf-8-sig") as f:
                DataN.clear()
                data = json.load(f)
                for deputy in data['votes']:
                    #### Vote
                    c.execute('INSERT INTO VoteDep VALUES(?,?,?)', (data['id'],deputy['deputy']['id'], deputy['result']))
                    faction = votes.setdefault(deputy['deputy']['faction']['title'], {'accept': 0, 'none': 0, 'decline': 0, 'abstain':0})
                    faction[deputy['result']] += 1
                    id[deputy['deputy']['faction']['title']]=deputy['deputy']['faction']['id']
                    
                    ##### Dep
                    deputy = deputy['deputy']
                    DataN[deputy['id']] = {'name': deputy['name'],'faction': {'FId': deputy['faction']['id'],'date': data['datetime'], 'datelast':'0'}}
                    c.execute('INSERT OR IGNORE INTO DeputysId VALUES(?,?)', (deputy['id'],deputy['name']))
                    lastdate = data['datetime']
                    if DataAll.setdefault(deputy['id'],'0')=='0':
                               DataAll[deputy['id']] = {'name': deputy['name'],'faction': {'FId': deputy['faction']['id'],'date': data['datetime'], 'datelast':'0'}}
                    #####
                               
                for keyv in votes.keys():
                    c.execute('INSERT OR IGNORE INTO FactionID VALUES(?,?)',(id[keyv],keyv))
                    c.execute('INSERT OR IGNORE INTO VoteID VALUES(?,?,?,?)',(data['id'], data['title'], data['datetime'], data['asozdUrl']))
                    c.execute('INSERT INTO Votes VALUES(?,?,?,?,?,?)', (data['id'], id[keyv], votes[keyv]['accept'], votes[keyv]['none'], votes[keyv]['decline'],
                                                                        votes[keyv]['abstain']))
                
                for deputy in Data.keys():
                    if DataN.setdefault(deputy,'0')=='0' and DataAll[deputy]['faction']['datelast']=='0':
                        DataAll[deputy]['faction']['datelast'] = Data[deputy]['faction']['date']
                        DataN.pop(deputy)
                Data = DataN.copy()
                votes.clear()
                faction.clear()
    
    for deputy in DataAll.keys():
        if DataAll[deputy]['faction']['datelast']== '0':
            DataAll[deputy]['faction']['datelast'] = lastdate
        c.execute('INSERT INTO Deputys VALUES(?,?,?,?)', (deputy, DataAll[deputy]['faction']['FId'], DataAll[deputy]['faction']['date'], DataAll[deputy]['faction']['datelast']))
    
                
    conn.commit()
    c.close()
    conn.close()
else:
    print ('Data Base already exist')
    pass

