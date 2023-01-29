import mysql.connector
import yaml
import praw
import re
from datetime import datetime
import random

# TODO: remove SQL syntax things and replace with mysql methods where possible

class Reddit5m5v:
    def __init__(self, bot_name, database, verbose=True, test=False):
        self.config = self._load_config()
        self.topics_to_scan = self.config["topics"]

        self.reddit = praw.Reddit(bot_name)
        self.database = database
        self.database_cursor = database.cursor(buffered=True)
        self.submission_table_name = "reddit"
        self.topics_comments_table = "reddit_comments"

        self.displayed_submissions = []

        self.verbose = verbose
        self.test = test

        if self.test:
            self.time_limit = 60
        else:
            self.time_limit = 10

    def display_table(self, table_name):
        sql = f"select * from {table_name}"
        self.database_cursor.execute(sql)

        print(f"Printing {table_name}.")
        for row in self.database_cursor.fetchall():
            print(row)

    def check_table_for_rows(self, table, submission):
        sql = f"select * from {table} where submission_id = %s"
        self.database_cursor.execute(sql, (submission.id,))
        rows = [i for i in self.database_cursor.fetchall()]
        return len(rows) >= 0

    def new_submission(self, submission):
        votes = submission.score
        comments = submission.num_comments
        title = submission.title.replace("'", "\\'")
        subreddit = submission.subreddit.display_name
        submission_id = submission.id
        submission_link = submission.permalink
        submission_text = submission.selftext.replace("'", "\\'") # TODO: figure out how images are handled and how we can get access to image URL

        if self.verbose:
            print(f"Entering new submission from r/{subreddit}: {title}")


        sql = f"""
            INSERT INTO {self.submission_table_name} 
                (submission_id, title, text, votes, comments, subreddit, link)
                VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        # TODO: prevent sql injection
        # TODO: add automatic created and updated timestamps on laravel migration


        self.database_cursor.execute(
            sql,
            (submission_id, title, submission_text, votes, comments, subreddit, submission_link))
        self.database.commit()

        if self.verbose:
            self.display_table(self.submission_table_name)
            print(f"r/{subreddit}: {title} added to {self.submission_table_name} table.")

        submission_topics = self.get_submission_topics(submission)

        for topic in submission_topics:
            self.write_new_topic(submission_id, submission_topics[topic], topic)

    def write_new_topic(self, submission_id, comments, topic):

        if self.verbose:
            print(f"Writing new topic: {topic}")

        comment_body = None
        random_comment_id = None
        comment_url = None
        comment_score = None
        if len(comments) > 0:
            random_comment = random.sample(comments, 1)
            comment_body = random_comment.body.replace("'", "\\'")
            random_comment_id = random_comment.id
            comment_url = self.get_comment_url(random_comment)
            comment_score = random_comment.score
        sql = f"""
        INSERT INTO {self.topics_comments_table} 
            (submission_id, example_comment_id, topic, num_comments, comment_text, comment_link, votes)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)"""
        self.database_cursor.execute(
            sql, (submission_id, random_comment_id, topic, len(comments), comment_body, comment_url, comment_score))

        #TODO: does this work?
        self.database.commit()

        self.display_table(self.topics_comments_table)

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

        if self.verbose:
            print(f"Updating submission r/{submission.subreddit.display_name}: {submission.title}")

        sql = f"""
            UPDATE {self.submission_table_name} 
            SET 
                votes = {votes}, 
                comments = {comments}
            WHERE submission_id = '{submission_id}'"""

        self.database_cursor.execute(sql)
        self.database.commit()

        if self.verbose:
            self.display_table(self.submission_table_name)
            print(f"Updated {self.submission_table_name} for r/{submission.subreddit.display_name}:"
                  f" {submission.title}")

            print(f"Updating topics for r/{submission.subreddit.display_name}: {submission.title}")

        submission_topics = self.get_submission_topics(submission)

        sql = f"select distinct * from {self.topics_comments_table} where submission_id = %s"
        # TODO: prevent sql injection for submission id, not for tpics comments table, that is not user input

        self.database_cursor.execute(sql, (submission.id,))
        self.database.commit()

        row = {}
        columns = [i[0] for i in self.database_cursor.description]
        for i in self.database_cursor.fetchall():
            for o in range(len(columns)):
                row[columns[o]] = i[o]
            print(row["topic"])
            print(submission_topics)

            if row["example_comment_id"]:
                comment = self.reddit.comment(row["example_comment_id"])
                votes = comment.score

                sql = f"""
                    UPDATE {self.topics_comments_table}
                    SET
                        votes = {votes},
                        num_comments = {len(submission_topics[row["topic"]])}
                    WHERE submission_id = '{submission_id}'
                          and topic = %s
                          and comment_id = '{row["example_comment_id"]}'""" #TODO: prevent sql injection for topic

                self.database_cursor.execute(sql, (row["topic"], ))
                self.database.commit()

                if self.verbose:
                    print(f"Updated {row['topic']}'s comment stats.")


            elif len(submission_topics[row["topic"]]):
                random_comment = random(submission_topics[row["topic"]], 1)

                sql = f"""
                    UPDATE {self.topics_comments_table}
                    SET
                        example_comment_id = '{random_comment.id}',
                        num_comments = {len(submission_topics[row["topics"]])},
                        comment_text = %s,
                        comment_link = '{self.get_comment_url(random_comment)}',
                        votes = {random_comment.score}
                    WHERE
                        submission_id = '{submission_id}'
                        and topic = %s'
                        """
                # TODO: prevent sql injection for comment text, topic

                self.database_cursor.execute(sql, (random_comment.body, row["topic"],))
                self.database.commit()

                if self.verbose:
                    print(f"Added new example comment for {row['topic']}.")
                    self.display_table(self.topics_comments_table)
            del submission_topics[row["topic"]]
        for t in submission_topics:
            self.write_new_topic(submission_id, submission_topics[t], t)


    def check_submission_topics(self, submission):
        submission_topics = {}
        for t in self.topics_to_scan:
            result = self.topic_scan(submission.selftext + submission.title, t)
            if result:
                submission_topics[t] = []
        if self.verbose:
            print(f"Scanned {submission.selftext + submission.title} found {[i for i in submission_topics]}.")
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

    def calculate_time_difference(self, timestamp):
        """
        given timestamp (UTC) calculate the time difference between the timestamp and right now.
        :param timestamp: UTC timestamp
        :return: time difference in hours.
        """
        current_time = datetime.utcnow()
        time_difference = current_time - datetime.utcfromtimestamp(timestamp)
        return time_difference.seconds / self.time_limit

    def _load_config(self):
        with open("config.yaml") as f:
            config = yaml.safe_load(f)
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
                where submission_id = '{submission.id}'"""
            self.database_cursor.execute(sql)
            self.database.commit()
            if self.verbose:
                print(f"Deleted {submission.id} from {table}.")

    def main(self):
        subreddits = self.config["subreddit_list"]
        subreddits = self.reddit.subreddit("+".join(subreddits))

        sql = f"select distinct submission_id from {self.submission_table_name};"
        self.database_cursor.execute(sql)
        logged_submissions = [i[0] for i in self.database_cursor.fetchall()]

        displayed_submissions = []
        n = 200
        for submission in subreddits.stream.submissions():
            # check submission
            print(logged_submissions)
            submission_topics = self.check_submission_topics(submission)

            sql = f"select distinct submission_id from {self.submission_table_name}"

            if submission_topics:
                # if valid, add to list
                if submission.id not in logged_submissions:
                    self.new_submission(submission)
                self.database_cursor.execute(sql)
                logged_submissions = [i[0] for i in self.database_cursor.fetchall()]

            for s in logged_submissions.copy():
                displayed_submission = self.reddit.submission(id=s)
                time_difference = self.calculate_time_difference(displayed_submission.created_utc)
                if time_difference >= 10:
                    # delete old records
                    self.delete_record(displayed_submission)
                elif s in logged_submissions:
                    self.update_submission(displayed_submission)
                elif s not in logged_submissions:
                    self.new_submission(displayed_submission)

                self.database_cursor.execute(sql)
                logged_submissions = [i[0] for i in self.database_cursor.fetchall()]
            if self.test:
                n -= 1
                if n == 0:
                    break


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
