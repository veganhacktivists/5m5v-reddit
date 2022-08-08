# 5m5v Reddit Bot

This is the 5m5v reddit bot. It reads through all the submissions that get through to reddit and sees if they're related 
to veganism and can benefit from having vegan activities (comments and votes). It deposits these submissions and related 
information into a mysql table. This table can then be read by the 5m5v reddit web app.

### Database
Database information can be set in `config.yaml`.

### Subreddits
Specify which subreddits to monitor in `config.yaml`.

### Topics
Specify what topics and the corresponding regex to match for in `config.yaml`.

### To run
Execute `python reddit_5m5v.py`.




