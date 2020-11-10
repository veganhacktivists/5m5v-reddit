import mysql.connector
import yaml
import praw
import re

class Database:
    def __init__(self, submission, bot_name, database):
        self.config = self._load_config()
        self.submission = submission
        self.reddit = praw.Reddit(bot_name)
        self.votes = self.score
        self.comments = self.submission.num_comments
        self.title = self.submission.title
        self.subreddit = self.subreddit.display_name
        self.submission_id = self.submission.id
        self.submission_link = self.submission.permalink
        self.created_at = self.submission.created_utc
        self.updated_at = None
        self.topics = self.get_current_topics()
        self.comments = None
        self.database = database
        self.database_cursor = database.cursor()

    def _load_config(self):
        with open("config.yaml") as f:
            config = yaml.load(f)
        return config

    def get_current_topics(self):
        sql = f"select distinct topic_id from submission_to_topic where post_id = {self.submission_id};"
        self.database_cursor.execute(sql)
        result = [i for i in self.database_cursor.fetchall()]
        return result

    def check_submission_text(self):
        pass

    def topic_scan(self, text, all_topics):
        topics = []
        for t in all_topics:
            pattern = re.compile(t)
            if pattern.search(text.lower()):
                topics.append(t)
        return topics

    def collect_comments(self):
        submission_list = []
        self.submission.comments.replace_more(limit=None)
        for comment in self.submission.comments.list():
            submission_list.append(comment)
        return submission_list

    def update_table(self):
        pass

    def update_record(self):
        pass

    def delete_record_from_table(self):
        pass

    def delete_record(self):
        pass

    def process(self):
        if self.comments is None:
            self.comments = {}
        comment_list = self.collect_comments()
        topics_left = [t for t in self.config["topics"] if t not in self.topics]
        s_topics = self.topic_scan(self.submission.selftext, topics_left)
        self.topics += s_topics
        for t in self.topics:
            topics_left = topics_left.remove(t)
        for c in comment_list:
            c_topics = self.topic_scan(c.body, topics_left)
            if c_topics:
                self.comments[c.id] = c_topics
                for t in self.topics:
                    topics_left = topics_left.remove(t)
        


if __name__ == "__main__":
    r = praw.Reddit("veganactivismbot")
    mydb = mysql.connector.connect(
        host=config["mysql_host"],
        user=config["mysql_user"],
        password=config["mysql_password"],
        database=config["mysql_database"])

    for s in r.subreddits.popular():
        print(s)
