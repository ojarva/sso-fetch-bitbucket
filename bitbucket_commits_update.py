"""
Downloads all commit timestamps for all repositories from Bitbucket
"""

# pylint: disable=C0301

import base64
import redis
import json
import datetime
import dateutil.parser
import httplib2
from config import Config
from instrumentation import *

def get_tzinfo_from_dt(dt1, dt2):
    dt1 = dt1.replace(tzinfo=None)
    dt2 = dt2.replace(tzinfo=None)
    diff = dt2 - dt1
    prefix = "+"
    if diff < datetime.timedelta(0):
        prefix = "-"
        diff = abs(diff)
    hours, minutes = diff.seconds // 3600, diff.seconds // 60 % 60
    return "%s%02d:%02d" % (prefix, hours, minutes)


def parse_timestamp_utc(time_str):
    parsed = dateutil.parser.parse(time_str)
    offset_str = str(parsed).rsplit("+", 1)
    prefix = "+"
    if len(offset_str) == 1:
        offset_str = offset_str[0].rsplit("-", 1)
        prefix = "-"
    if len(offset_str) == 2:
        offset_str = prefix + offset_str[1]
    else:
        offset_str = ""
    if hasattr(parsed.tzinfo, "_offset"):
        offset = parsed.tzinfo._offset
    else:
        offset = datetime.timedelta(0)
    parsed = parsed.replace(tzinfo=None) - offset
    return (parsed, offset_str)
    

class BitbucketUpdate:
    def __init__(self, username, password, organization):
        self._db = None
        self.organization = organization
        auth = base64.encodestring( username + ':' + password)
        self.headers = {"Authorization": "Basic "+ auth}
        self.http = httplib2.Http(disable_ssl_certificate_validation=True)
        self.config = Config()
        self.redis = redis.Redis(host=self.config.get("redis-hostname"), port=self.config.get("redis-port"), db=self.config.get("redis-db"))
        self.post_queue = []

    @timing("bitbucket.update.get_repositories")
    def get_repositories(self):
        statsd.incr("bitbucket.update.get_repositories")
        repos = []
        url = "https://bitbucket.org/api/1.0/user/repositories"
        (_, content) = self.http.request(url, "GET", headers=self.headers)
        repos = json.loads(content)
        return repos

    @timing("bitbucket.update.get_commits")
    def get_commits(self, repo_name, last_commit):
        statsd.incr("bitbucket.update.get_commits")
        url = "https://bitbucket.org/api/1.0/repositories/%s/%s/changesets?limit=30" % (self.organization, repo_name)
        if last_commit:
            url += "&start=%s" % last_commit
        (_, content) = self.http.request(url, "GET", headers=self.headers)
        commits = json.loads(content)
        return commits

    def _process_repo(self, name, last_processed):
        last_processed_save = last_processed
        last_commit = None
        while True:
            commits = self.get_commits(name, last_commit)
            if not "changesets" in commits:
                break
            if len(commits["changesets"]) == 0:
                break
            last_commit = commits["changesets"][0]["node"]
            for commit in commits["changesets"]:
                timestamp = commit["utctimestamp"]
                tzinfo = get_tzinfo_from_dt(dateutil.parser.parse(commit["utctimestamp"]), dateutil.parser.parse(commit["timestamp"]))
                if timestamp > last_processed_save:
                    last_processed_save = timestamp
                author = commit["raw_author"]
                if not "<" in author:
                    continue
                author = author.split("<")
                author = author[1].replace(">", "")
                if not author.endswith(self.config.get("email-domain")):
                    continue
                if timestamp > last_processed:
                    self.post({"system": "bitbucket-commits", "timestamp": timestamp, "username": author, "data": name, "is_utc": True, "tzinfo": tzinfo})
                else:
                    return last_processed_save
            if len(commits["changesets"]) == 1:
                # Only one commit -- namely, start. Return.
                return last_processed_save

    @timing("bitbucket.update.main")
    def process(self):

        last_processed_save = None
        statsd.incr("bitbucket.update.main")
        repositories = self.get_repositories()
        for repo in repositories:
            last_change = repo.get('last_updated')
            name = repo.get("slug")
            if not name:
                continue
            repo_key = "bitbucket-%s-" % name
            last_processed = self.redis.get(repo_key+"pushed_at")
            if not last_processed:
                last_processed = "1970-01-01T00:00:00"
            if last_processed >= last_change:
                continue
            last_processed_save = self._process_repo(name, last_processed)

            self.redis.set(repo_key+"pushed_at", last_processed_save)
            self.post_finished()

    def post_finished(self):
        self.post()

    @timing("bitbucket.update.post")
    def post(self, data = None):
        if data:
            self.post_queue.append(data)
        if len(self.post_queue) > 100 or (len(self.post_queue) > 0 and data is None):
            statsd.incr("bitbucket.update.post.request")
            (_, cont) = self.http.request(self.config.get("server-url"), "POST", body=json.dumps(self.post_queue))
            if cont == "OK":
                self.post_queue = []
            else:
                return False

def main():
    config = Config()
    bitbucket = BitbucketUpdate(config.get("username"), config.get("password"), config.get("organization"))
    bitbucket.process()

if __name__ == '__main__':
    main()
