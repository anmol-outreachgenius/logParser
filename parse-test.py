from google.cloud import storage
import re
import pprint
from urllib.parse import parse_qs
import psycopg2
import os
import json
import datetime
import ipcheck

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath('')+'/key.json'

storage_client = storage.Client()


def list_blobs():
    bucket_name = "pixel-logs-outreach-genius"

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    return blobs


parsed_urls = []

for blob in list_blobs():
    downloaded_blobs = blob.download_as_string().decode("utf-8").split("\n")
    for item in downloaded_blobs:
        if len(item) > 2:
            parsed_blob = json.loads(item)
            parsed_url = parse_qs(
                parsed_blob["httpRequest"]["requestUrl"].split("?")[1]
            )
            parsed_url["ip"] = parsed_blob["httpRequest"]["remoteIp"]
            parsed_url["latency"] = parsed_blob["httpRequest"]["latency"]
            parsed_urls.append(parsed_url)

pprint.pp(parsed_urls[0])

conn = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="postgres",
    host="pixel-log.cxgy2o6qilhd.us-west-2.rds.amazonaws.com",
    port="5432",
)

eventmap = {
    "ctaClick": "cta_clicks",
    "formInteraction": "form_interactons",
    "videoEngagement": "video_engagements",
    "socialMediaInteraction": "social_media_clicks",
    "download": "file_downloads",
    "productReview": "product_reviews",
    "emailSubscribe": "email_subscriptions",
}


def sanitize_input(inputData):
    # Use regular expression to remove non-alphanumeric characters
    sanitized_data = re.sub(r"[^a-zA-Z0-9\-\_\s]", "", inputData)
    return sanitized_data


def createColumn(
    sessionId,
    timestamp,
    pid,
    uid,
    url,
    referer,
    screen_width,
    screen_height,
    country,
    ASN,
    city,
    user_agent,
    latency,
    ip_address,
    browser,
    is_mobile,
):
    query = "INSERT INTO Logs (sessionId, visited_at, pid, uid,url,referer,screen_width,screen_height,country,ASN,city,user_agent,latency,ip_address,browser,is_mobile) VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s)"
    cur = conn.cursor()
    cur.execute(
        query,
        (
            sessionId,
            timestamp,
            pid,
            uid,
            url,
            referer,
            screen_width,
            screen_height,
            country,
            ASN,
            city,
            user_agent,
            latency,
            ip_address,
            browser,
            is_mobile,
        ),
    )
    print("created column")
    conn.commit()


def logEvent(sessionId, event):
    query = f"UPDATE Logs SET {event} = {event} + 1 WHERE sessionId = %s"
    cur = conn.cursor()
    cur.execute(query, (sessionId,))
    conn.commit()


def setValue(sessionId, column, value):
    query = f"UPDATE Logs SET {column} = %s WHERE sessionId = %s"
    cur = conn.cursor()
    cur.execute(query, (value, sessionId))
    conn.commit()


def handleData(data):
    if data["ev"][0] == "pageView":
        createColumn(
            data["sessionId"][0],
            datetime.datetime.fromtimestamp(int(data["ts"][0]) / 1000),
            data["pid"][0],
            data["uid"][0],
            data["dl"][0],
            "TODO",
            int(data["sr"][0].split("x")[0]),
            int(data["sr"][0].split("x")[1]),
            ipcheck.get_country(data["ip"]),
            ipcheck.get_asn(data["ip"]),
            ipcheck.get_city(data["ip"]),
            data["ua"][0],
            data["latency"],
            data["ip"],
            data["bn"][0],
            data["md"][0],
        )

    if data["ev"][0] == "sessionEnd":
        setValue(
            data["sessionId"][0], "session_duration", eval(data["ed"][0])["timeSpent"]
        )
    try:
        logEvent(data["sessionId"][0], eventmap[data["ev"][0]])
    except Exception as e:
        print(e)


for data in parsed_urls:
    try:
        handleData(data)
    except Exception as e:
        conn.close()
        print(e, e.args)
        break
