from pymongo import MongoClient
import time
import requests
import codecs


URL = 'http://api.themoviedb.org/3/'
CONFIG_URL = 'http://api.themoviedb.org/3/configuration'
EXAMPLE = ('https://api.themoviedb.org/3/configuration?'
           'api_key=78ef9d5bb5afba0881ccda78508a961a')
EXAMPLE2 = ('https://api.themoviedb.org/3/movie/32612'
            '?api_key=78ef9d5bb5afba0881ccda78508a961a')
API_KEY = '78ef9d5bb5afba0881ccda78508a961a'
MOVIE = 'movie/'
THUMBNAIL = 'w92'
LARGE = 'w500'
BASE_URL = 'http://image.tmdb.org/t/p/'
KEY_APPEND = '?api_key=' + API_KEY

FILE_CACHE = 'tmdb-data.txt'


def failedTmdbLookup(movie_id):
    client = MongoClient()
    db = client.movie_db
    movies = db.movies
    movie = movies.find_one({'_id': movie_id})
    movie['tmdbLookupFailed'] = True
    movies.save(movie)
    return movie


def updateMovie(data, base_url, movie_id):
    client = MongoClient()
    db = client.movie_db
    movies = db.movies
    movie = movies.find_one({'_id': movie_id})
    updated = False
    posterThumbnailHref = ''
    posterHref = ''
    if data['poster_path'] is not None:
        posterThumbnailHref = (base_url + THUMBNAIL
                                        + data['poster_path'])
        posterHref = base_url + LARGE + data['poster_path']
    if movie is not None:
        updated = True
        movie['posterPath'] = data['poster_path']
        movie['backdropPath'] = data['backdrop_path']
        movie['posterThumbnailHref'] = posterThumbnailHref
        movie['posterHref'] = posterHref
        movies.save(movie)
    else:
        print('  mongo couldn\'t find movie id {}'.format(movie_id))
    return updated


def fetch(pid, f, base_url, movie_id, tmdb_id, title=''):
    start = time.time()
    req_url = URL + MOVIE + tmdb_id + KEY_APPEND
    response = requests.get(req_url)
    # print('Response: status: {} text: {}'.format(response.status, text))
    updated = False
    if response.status_code == 200:
        data = response.json()
        f.write(response.text + '\n')
        updated = updateMovie(data, base_url, movie_id)
        time.sleep(0.3)
    elif response.status_code == 404:
        failedTmdbLookup(movie_id)
        print('TMDB API couldn\'t find movie {} {} ({}).'
              .format(title, movie_id, tmdb_id))
    elif response.status_code == 429:
        print('**** OVER TIME Returned status {} **** '
              .format(response.status_code))
        time.sleep(4)
    else:
        print('Returned status {}'.format(response.status_code))
    end_time = time.time()
    # print('{} fetched {} {} in {:0.2f} sec ({})'.
    #       format(pid, title, movie_id, end_time-start, str(updated)))


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
                fetch(index, f, BASE_URL, movie['_id'],
                      movie['tmdbId'], movie['title'])
                count += 1
                index += 1
    return count


client = MongoClient()
db = client.movie_db
movies = db.movies
query = {
    'posterPath': {'$not': {'$exists': True}},
    'tmdbLookupFailed': {'$not': {'$eq': True}},
    'tmdbId': {
        '$exists': True,
        '$not': {'$eq': ''}
    }
}
count_cursor = movies.find(query)
total = count_cursor.count()
count_cursor.close()
cursor = movies.find(query)

STEP_SIZE = 20
loop = 1
count = 0

with codecs.open(FILE_CACHE, 'a+', 'utf-8') as f:
    while cursor.alive:
        print('Loop {}, imported {} / {}'.format(loop, count, total))
        count += main(f, cursor, STEP_SIZE)
        time.sleep(2)
        loop += 1
