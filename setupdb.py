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
DROP TABLE IF EXISTS Logs;
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
    screen_width int,
    screen_height int,
    latency varchar(100),
    user_agent varchar(400),
    country varchar(200),
    city varchar(200),
    ASN varchar(200),
    video_engagements int default 0,
    social_media_clicks int default 0,
    file_downloads int default 0,
    product_reviews int default 0,
    email_subscriptions int default 0,
    browser varchar(400),
    is_mobile bool,
    ip_address varchar(100)
);
''')

db.commit()

db.close()