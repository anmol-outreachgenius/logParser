import os
import re
import sqlite3
from urllib.parse import parse_qs
import pprint
import time
# import numpy as np
db = sqlite3.connect('./database/MyDatabase.sqlite')
eventmap = {
"ctaClick":"cta_clicks",
"formInteraction":"form_interactons",
"videoEngagement":"video_engagements",
"socialMediaInteraction":"social_media_clicks",
"download":"file_downloads",
"productReview":"product_reviews",
"emailSubscribe":"email_subscriptions"
}

def sanitize_input(inputData):
    # Use regular expression to remove non-alphanumeric characters
    sanitized_data = re.sub(r'[^a-zA-Z0-9\-\_\s]', '', inputData)
    return sanitized_data

def createColumn(sessionId,timestamp,pid,uid):
    query = f'INSERT INTO Logs (sessionId,visited_at,pid,uid) VALUES (?,?,?,?)'
    cur = db.cursor();
    cur.execute(query, [sessionId,timestamp,pid,uid]);
    print('created column')
    db.commit();

def logEvent(sessionId,event):
    query = f'UPDATE Logs SET {event} = {event} + 1 WHERE sessionId = ?'
    cur = db.cursor();
    cur.execute(query, [sessionId]);
    db.commit();

def setValue(sessionId,column,value):
    query = f'UPDATE Logs SET {column} = ? WHERE sessionId = ?'
    cur = db.cursor();
    cur.execute(query, [value ,sessionId]);
    db.commit();

def handleData(data):
    if (data["ev"][0] == 'pageView') :
        createColumn(data['sessionId'][0],int(data['ts'][0]),data['pid'][0],data['uid'][0])
        
    if (data["ev"][0] == 'sessionEnd') :
        setValue(data['sessionId'][0],'session_duration',eval(data['ed'][0])['timeSpent'])
    try:
        logEvent(data['sessionId'][0],eventmap[data['ev'][0]])
    except Exception as e:
        print(e)




while 1:
    files = os.listdir('./logs')
    raw_logs = []
    for filename in files:
        with open('./logs/'+filename, 'r') as f:
            print('"requestUrl":"https://pixel.outreachgenius.ai/pixel.gif"'+f.readline())
            # data = parse_qs(f.readline().split('?')[1])
            # os.remove('./logs/'+filename)
            # handleData(data)
            # pprint.pp([data])
    time.sleep(10)
    
    
    