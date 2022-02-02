# bts-hub

[![bts-hub-daily-scrape](https://github.com/peteb206/bts-hub/actions/workflows/bts-hub-daily-scrape.yml/badge.svg)](https://github.com/peteb206/bts-hub/actions/workflows/bts-hub-daily-scrape.yml)

## Open in Heroku
https://bts-hub.herokuapp.com

## Open Locally
```
cd <bts-hub directory>
FLASK_APP=bts-hub.py FLASK_ENV=development flask run
```
If the following error is raised, run bts-hub.py like a regular python program to see what went wrong:
```
Error: While importing 'app', an ImportError was raised.
```
http://localhost:<port\>