import mysql.connector
import yaml
import praw
import re
from datetime import datetime
import random

class Reddit5m5v:
    def __init__(self, bot_name, database):
        self.config = self._load_config()
        self.topics_to_scan = self.config["topics"]

        self.reddit = praw.Reddit(bot_name)
        self.database = database
        self.database_cursor = database.cursor()
        self.submission_table_name = None
        self.topics_comments_table = None

        self.displayed_submissions = []

    def new_submission(self, submission):
        votes = submission.score
        comments = submission.num_comments
        title = submission.title
        subreddit = submission.subreddit.display_name
        submission_id = submission.id
        submission_link = submission.permalink
        submission_text = submission.selftext

        sql = f"""
            INSERT INTO {self.submission_table_name} 
                (submission_id, title, text, votes, comments, subreddit, link)
                VALUES ({submission_id}, {title}, {submission_text}, {votes}, {comments}, {subreddit},
            {submission_link});"""

        self.database_cursor.execute(sql)
        self.database.commit()

        submission_topics = self.get_submission_topics(submission)

        for topic in submission_topics:
            self.write_new_topic(submission_id, submission_topics[topic], topic)

    def write_new_topic(self, submission_id, comments, topic):
        if len(comments) > 0:
            random_comment = random.sample(comments, 1)
            sql = f"""
            INSERT INTO {self.topics_comments_table} 
                (submission_id, example_comment_id, topic, num_comments, comment_text, comment_link, votes)
            VALUES
                ({submission_id}, {random_comment.id}, {topic}, {len(comments)}, 
            {random_comment.body}, {self.get_comment_url(random_comment)}, {random_comment.score})
                            """
        else:
            sql = f"""
            INSERT INTO {self.topics_comments_table} 
                (submission_id, example_comment_id, topic, num_comments, comment_text, comment_link, votes)
            VALUES
                ({submission_id}, null, {topic}, {len(submission_topics[topic])}, 
            null, null, null)
            """
        self.database_cursor.execute(sql)
        self.database.commit()

    def get_comment_url(self, comment):
        """

        http://www.reddit.com/comments/1p3qau/_/ccz05xk
        :return:
        """

        submission_id = comment.submission.id
        comment_id = comment.id

        return f"https://www.reddit.com/comments/{submission_id}/_/{comment_id}?context=3"

    def update_submission(self, submission):
        votes = submission.score
        comments = submission.num_comments
        submission_id = submission.id


        sql = f"""
            UPDATE {self.submission_table_name} 
            SET 
                votes = {votes}, 
                comments = {comments},
            WHERE submission_id = {submission_id};"""

        self.database_cursor.execute(sql)

        submission_topics = self.get_submission_topics(submission)

        sql = f"select distinct * from submission_to_topic where submission_id = {submission_id};"
        self.database_cursor.execute(sql)
        row = {}
        columns = [i[0] for i in self.database_cursor.description]
        for i in self.database_cursor.fetchall():
            for o in range(len(columns)):
                row[columns[o]] = i[o]

            if row["example_comment_id"]:
                comment = self.reddit.comment(row["example_comment_id"])
                votes = comment.score
                sql = f"""
                    UPDATE {self.topics_comments_table}
                    SET
                        votes = {votes},
                        num_comments = {len(submission_topics[row["topic"]])}
                    WHERE submission_id = {submission_id}
                          and topic = {row["topic"]}
                          and comment_id = {row["example_comment_id"]};"""
            elif len(submission_topics[row["topics"]]):
                random_comment = random(submission_topics[row["topics"]], 1)
                sql = f"""
                    UPDATE {self.topics_comments_table}
                    SET
                        example_comment_id = {random_comment.id},
                        num_comments = {len(submission_topics[row["topics"]])},
                        comment_text = {random_comment.body},
                        comment_link = {self.get_comment_url(random_comment)},
                        votes = {random_comment.score}
                    WHERE
                        submission_id = {submission_id}
                        and topic = {row["topic"]}
                        """
            del submission_topics[row["topics"]]
        for t in submission_topics:
            self.write_new_topic(submission_id, submission_topics[t], t)


    def check_submission_topics(self, submission):
        submission_topics = {}
        for t in self.topics_to_scan:
            result = self.topic_scan(submission.selftext + submission.title, t)
            if result:
                submission_topics[t] = []

        return submission_topics

    def get_submission_topics(self, submission):

        submission_topics = self.check_submission_topics(submission)

        comments = self.collect_comments(submission)
        for c in comments:
            for t in submission_topics:
                result = self.topic_scan(c.body, t)
                if result:
                    submission_topics[t].append(c)

        return submission_topics

    def calculate_time_difference(timestamp):
        """
        given timestamp (UTC) calculate the time difference between the timestamp and right now.
        :param timestamp: UTC timestamp
        :return: time difference in hours.
        """
        current_time = datetime.utcnow()
        time_difference = current_time - datetime.utcfromtimestamp(timestamp)
        return time_difference.seconds / 60

    def _load_config(self):
        with open("config.yaml") as f:
            config = yaml.load(f)
        return config

    def topic_scan(self, text, topic):
        """
        Search for topics in texts and returns those topics as a list.

        :param text: [string] text from a comment or a submission
        :param all_topics: [list] topics to search for
        :return: [list] topics found in the text
        """
        pattern = re.compile(self.config["topics"][topic])
        if pattern.search(text.lower()):
            return True
        return False

    def collect_comments(self, submission):
        """
        read all comments and match them to topics.
        :return:
        """
        comment_list = list(submission.comments.replace_more(limit=None))

        return comment_list

    def delete_record(self, submission):
        for table in [self.submission_table_name, self.topics_comments_table]:
            sql = f"""  
                DELETE from {table}
                where submission_id = {submission.id}"""
            self.database_cursor.execute(sql)
            self.database.commit()

    def main(self):
        subreddits = config["subreddit_list"]
        subreddits = self.reddit.subreddit("+".join(subreddits))
        displayed_submissions = []
        for submission in subreddits.stream.submissions():
            # check submission
            submission_topics = self.check_submission_topics(submission)
            if submission_topics:
                # if valid, add to list
                displayed_submissions.append(submission)

            sql = f"select distinct submission_id from {self.submission_table_name};"
            self.database_cursor.execute(sql)
            logged_submissions = [i for i in self.database_cursor.fetchall()]

            for s in displayed_submissions.copy():
                time_difference = self.calculate_time_difference(s.created_utc)
                if time_difference >= 10:
                    # delete old records
                    self.delete_record(s)
                    displayed_submissions.pop(0)
                elif s in logged_submissions:
                    self.update_submission(s)
                elif s not in logged_submissions:
                    self.new_submission(s)


if __name__ == "__main__":
    with open("config.yaml", "rb") as f:
        config = yaml.safe_load(f)
    mydb = mysql.connector.connect(
        host=config["mysql_host"],
        user=config["mysql_user"],
        password=config["mysql_password"],
        database=config["mysql_database"])

    reddit5m5v = Reddit5m5v("veganactivismbot", mydb)
    reddit5m5v.main()
