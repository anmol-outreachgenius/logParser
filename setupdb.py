import psycopg2


db= psycopg2.connect(
    database="postgres",
    user="postgres",
    password="postgres",
    host="pixel-log.cxgy2o6qilhd.us-west-2.rds.amazonaws.com",
    port='5432'
)

cur = db.cursor()

cur.execute('''
            CREATE Table Logs (
uid varchar(200),
pid varchar(200),
sessionId varchar(200),
visited_at timestamp,
session_duration int default 0,
cta_clicks int default 0,
form_interactons int default 0,
scroll_depth float default 0,
referer varchar(400),
url varchar(400),
ip varchar(100),
video_engagements int default 0,
social_media_clicks int default 0,
file_downloads int default 0,
product_reviews int default 0,
email_subscriptions int default 0
);
            
            ''')

db.commit()

db.close()