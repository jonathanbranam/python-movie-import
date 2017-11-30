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


def fetch(pid, f, base_url, movie_id, tmdb_id):
    start = time.time()
    req_url = URL + MOVIE + tmdb_id + KEY_APPEND
    response = requests.get(req_url)
    # print('Response: status: {} text: {}'.format(response.status, text))
    updated = False
    if response.status_code == 200:
        data = response.json()
        f.write(response.text + '\n')
        updated = updateMovie(data, base_url, movie_id)
    elif response.status_code == 404:
        print('TMDB API couldn\'t find movie {} ({}).'
              .format(movie_id, tmdb_id))
    elif response.status_code == 429:
        print('OVER TIME Returned status {}'.format(response.status_code))
        time.sleep(4)
    else:
        print('Returned status {}'.format(response.status_code))
    end_time = time.time()
    print('{} fetched {} in {:0.2f} sec ({})'.
          format(pid, movie_id, end_time-start, str(updated)))


def main(f, cursor, step):
    for index in range(1, step+1):
        if cursor.alive:
            movie = cursor.next()
            fetch(index, f, BASE_URL, movie['_id'], movie['tmdbId'])
            index += 1


client = MongoClient()
db = client.movie_db
movies = db.movies
cursor = movies.find({'posterPath': {'$not': {'$exists': True}}})

STEP_SIZE = 20
loop = 1
with codecs.open(FILE_CACHE, 'w', "utf-8") as f:
    while cursor.alive:
        print('Loop {} through event loop.'.format(loop))
        main(f, cursor, STEP_SIZE)
        time.sleep(2)
        loop += 1
