# %%
# !pip install --upgrade google-cloud-storage google-cloud-bigquery google-cloud-bigquery-storage
# !gcloud auth application-default login
from google.cloud import storage
import re
import pprint
from urllib.parse import parse_qs
import psycopg2
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getcwd()+'/key.json'

# %%
storage_client = storage.Client()


# %%
def list_blobs():
    bucket_name = "pixel-logs-outreach-genius"

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    return blobs

# %%
urls = []

for blob in list_blobs():
    downloaded_blobs = blob.download_as_string().decode('utf-8')
    urls = urls + [parse_qs(x) for x in (re.findall(r'"requestUrl":"https://pixel.outreachgenius.ai/pixel.gif\?([^"]*)"',downloaded_blobs))]
    blob.delete()

pprint.pp(urls)

# %%
conn= psycopg2.connect(
    database="postgres",
    user="postgres",
    password="postgres",
    host="pixel-log.cxgy2o6qilhd.us-west-2.rds.amazonaws.com",
    port='5432'
)

import datetime

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

def createColumn(sessionId, timestamp, pid, uid,url,location,referer):
    query = 'INSERT INTO Logs (sessionId, visited_at, pid, uid,url,location,referer) VALUES (%s, %s, %s, %s, %s,%s,%s)'
    cur = conn.cursor()
    cur.execute(query, (sessionId, timestamp, pid, uid,url,location,referer))
    print('created column')
    conn.commit()

def logEvent(sessionId, event):
    query = f'UPDATE Logs SET {event} = {event} + 1 WHERE sessionId = %s'
    cur = conn.cursor()
    cur.execute(query, (sessionId,))
    conn.commit()

def setValue(sessionId, column, value):
    query = f'UPDATE Logs SET {column} = %s WHERE sessionId = %s'
    cur = conn.cursor()
    cur.execute(query, (value, sessionId))
    conn.commit()

def handleData(data):
    if (data["ev"][0] == 'pageView') :
        createColumn(data['sessionId'][0],datetime.datetime.fromtimestamp(int(data['ts'][0])/1000),data['pid'][0],data['uid'][0],data['dl'][0],'TODO','TODO')
        
    if (data["ev"][0] == 'sessionEnd') :
        setValue(data['sessionId'][0],'session_duration',eval(data['ed'][0])['timeSpent'])
    try:
        logEvent(data['sessionId'][0],eventmap[data['ev'][0]])
    except Exception as e:
        print(e)


# %%
for data in urls:
    try:
        handleData(data)
    except Exception:
        print(Exception)
conn.close()

# %%


# %%



