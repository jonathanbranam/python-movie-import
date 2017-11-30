from pymongo import MongoClient
import time
import requests
import codecs
from datetime import datetime


URL = 'http://api.themoviedb.org/3/'
CONFIG_URL = 'http://api.themoviedb.org/3/configuration'
EXAMPLE = ('https://api.themoviedb.org/3/configuration?'
           'api_key=78ef9d5bb5afba0881ccda78508a961a')
EXAMPLE2 = ('https://api.themoviedb.org/3/movie/32612'
            '?api_key=78ef9d5bb5afba0881ccda78508a961a')
API_KEY = '78ef9d5bb5afba0881ccda78508a961a'
MOVIE = 'movie/'
CREDITS = '/credits'
THUMBNAIL = 'w92'
LARGE = 'w500'
BASE_URL = 'http://image.tmdb.org/t/p/'
KEY_APPEND = '?api_key=' + API_KEY

FILE_CACHE = 'tmdb-credits.txt'


def fetch(pid, f, base_url, movie_id, tmdb_id, title=''):
    start = time.time()
    req_url = URL + MOVIE + tmdb_id + CREDITS + KEY_APPEND
    response = requests.get(req_url)
    # print('Response: status: {} text: {}'.format(response.status, text))
    success = False
    if response.status_code == 200:
        success = True
        # data = response.json()
        f.write(response.text + '\n')
        time.sleep(0.3)
    elif response.status_code == 404:
        print('TMDB API couldn\'t find movie {0} {1} ({2}).'
              .format(title, movie_id, tmdb_id))
    elif response.status_code == 429:
        print('OVER TIME Returned status {}'.format(response.status_code))
        time.sleep(4)
    else:
        print('Returned status {}'.format(response.status_code))
    end_time = time.time()
    print('{} fetched {} ({}) in {:0.2f} sec ({})'.
          format(pid, title, movie_id, end_time-start, str(success)))


def hasValidTmdbId(movie):
    if movie['tmdbId'] is None:
        return False
    elif type(movie['tmdbId']) is not str:
        return False
    elif len(movie['tmdbId']) <= 0:
        return False
    else:
        return True


def main(f, cursor, step):
    count = 0
    for index in range(1, step+1):
        if cursor.alive:
            movie = cursor.next()
            if hasValidTmdbId(movie):
                fetch(index, f, BASE_URL,
                      movie['_id'], movie['tmdbId'], movie['title'])
                count += 1
                index += 1
    return count


for hour in range(0, 0):
    # An hour
    print('Sleeping for hour {}... {}'
          .format(hour+1,
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    for minute in range(0, 60):
        print('Sleeping for minute {}... {}'
              .format(minute+1,
                      datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        # A minute
        time.sleep(60)

client = MongoClient()
db = client.movie_db
movies = db.movies
count_cursor = movies.find({'hasCastData': {'$not': {'$eq': True}}})
total = count_cursor.count()
count_cursor.close()
print('{} movies missing cast data'.format(total))
cursor = movies.find({'hasCastData': {'$not': {'$eq': True}}})

STEP_SIZE = 20
loop = 1
count = 0

with codecs.open(FILE_CACHE, 'a+', 'utf-8') as f:
    while cursor.alive:
        print('Loop {}, imported {} / {}'.format(loop, count, total))
        count += main(f, cursor, STEP_SIZE)
        time.sleep(3.5)
        loop += 1
