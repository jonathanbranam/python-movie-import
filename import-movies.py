import csv
from pymongo import InsertOne, UpdateOne, MongoClient
import time
import json

client = MongoClient()
# client = MongoClient('localhost', 27017)
# client = MongoClient('mongodb://localhost:27017')

db = client.movie_db

BULK_INSERT_COUNT = 1000
BULK_UPDATE_COUNT = 1000
BULK_CAST_COUNT = 1000


def importMovies(file_name, db):
    db.drop_collection('movies')
    movies = db.movies
    start = time.perf_counter()
    count = 0
    with open(file_name) as movieFile:
        movieReader = csv.DictReader(movieFile)
        # columns: movieId,title,genres
        # next(movieReader, None)
        movieList = []
        for row in movieReader:
            genres = []
            if row['genres'] != '(no genres listed)':
                genres = row['genres'].split('|')

            movie_data = {
                'ml_id': row['movieId'],
                'title': row['title'],
                'ml_title': row['title'],
                'genres': genres,
                'ml_genres': genres,
            }
            movieList.append(movie_data)
            count += 1
            if len(movieList) >= BULK_INSERT_COUNT:
                movies.insert_many(movieList)
                cur_time = time.perf_counter()
                print('Inserted {0} ({1} in {2:.2f} secs) movies...'
                      .format(len(movieList), count, cur_time-start))
                movieList = []
            # print(movie_data)
        if len(movieList) > 0:
            movies.insert_many(movieList)
            cur_time = time.perf_counter()
            print('Inserted {0} ({1} in {2:.2f} secs) movies...'
                  .format(len(movieList), count, cur_time-start))

    index_start = time.perf_counter()
    print('Indexing {0} movies...'.format(count))
    movies.create_index('ml_id')
    movies.create_index('title')
    index_end = time.perf_counter()
    print('Indexed {0} movies in {1:0.2f} secs'.format(count,
          index_end-index_start))
    import_end = time.perf_counter()
    print('Imported {0} total movies in {1:.0f} secs...'.format(count,
          import_end-start))
    return count


def castCreateOrUpdate(member, movie, cast):
    lookup = cast.find_one({'tmdb_id': member['id']})
    if lookup is None:
        return InsertOne({
            'tmdb_id': member['id'],
            'name': member['name'],
            'gender': member['gender'],
            'roles': [{
                'movie_id': movie['_id'],
                'order': member['order'],
                'character': member['character'],
            }]
        })
    else:
        return UpdateOne(
            {'_id': lookup['_id']},
            {'$push': {
                'roles': {
                    'movie_id': movie['_id'],
                    'order': member['order'],
                    'character': member['character'],
                }
            }}
        )


def importCastCrew(file_name, db):
    db.drop_collection('cast')
    movies = db.movies
    cast = db.cast
    cast.create_index('tmdb_id')
    cast.create_index('name')
    cast.create_index('roles.movie_id')
    cast_count = 0
    missing_movies = 0
    start = time.perf_counter()
    with open(file_name) as castFile:
        castReader = csv.DictReader(castFile)
        # columns: movie_id,title,cast,crew
        castList = []
        for row in castReader:
            movie = movies.find_one({'tmdb_id': row['movie_id']})
            if movie is None:
                missing_movies += 1
                # print("Couldn't find movie {0} with id {1}."
                #       .format(row['title'], row['movie_id']))
            if movie is not None:
                cast_list = json.loads(row['cast'])
                # print('{0} has {1} cast members'.format(movie['title'],
                #       len(cast_list)))
                for member in cast_list:
                    # ['cast_id', 'character', 'credit_id', 'gender', 'id',
                    #  'name', 'order']
                    cast_count += 1
                    op = castCreateOrUpdate(member, movie, cast)
                    castList.append(op)
            # seems we need to insert after every movie to avoid duplicates
            if len(castList) > 0:
                # print(castList)
                cast.bulk_write(castList, ordered=False)
                cur_time = time.perf_counter()
                print('Updated {0} ({1} in {2:.2f} secs) movies...'
                      .format(len(castList), cast_count,
                              cur_time-start))
                castList = []
        if len(castList) > 0:
            # print(castList)
            cast.bulk_write(castList, ordered=False)
            cur_time = time.perf_counter()
            print('Updated {0} ({1} in {2:.2f} secs) movies...'
                  .format(len(castList), cast_count,
                          cur_time-start))
            castList = []
    print("Couldn't find {0} movies...".format(missing_movies))


def updateMovieLinks(file_name, db, count):
    movies = db.movies
    update_count = 0
    start = time.perf_counter()
    with open(file_name) as linkFile:
        linkReader = csv.DictReader(linkFile)
        # columns: movieId,imdbId,tmdbId
        updateList = []
        for row in linkReader:
            movie = movies.find_one({'ml_id': row['movieId']})
            if movie is not None:
                update_count += 1
                # Updating each movie once at a time is far too slow
                # movie['imdb_id'] = row['imdbId']
                # movie['tmdb_id'] = row['tmdbId']
                # movies.save(movie)
                updateList.append(UpdateOne(
                    {'_id': movie['_id']},
                    {'$set': {
                        'imdb_id': row['imdbId'],
                        'tmdb_id': row['tmdbId']
                    }}
                ))

            if len(updateList) >= BULK_UPDATE_COUNT:
                # print(updateList)
                movies.bulk_write(updateList, ordered=False)
                cur_time = time.perf_counter()
                time_remaining = (cur_time-start) / update_count \
                    * (count-update_count)
                print(('Updated {0} ({1} / {2} in {3:.2f} secs) movies...'
                      ' {4:.0f} secs remaining')
                      .format(len(updateList), update_count, count,
                              cur_time-start, time_remaining))
                updateList = []
        if len(updateList) > 0:
            movies.bulk_write(updateList, ordered=False)
            cur_time = time.perf_counter()
            print('Updated {0} ({1} / {2} in {3:.2f} secs) movies...'
                  .format(len(updateList), update_count, count,
                          cur_time-start))

    index_start = time.perf_counter()
    print('Indexing {0} movies...'.format(count))
    movies.create_index('tmdb_id')
    movies.create_index('imdb_id')
    index_end = time.perf_counter()
    print('Indexed {0} movies in {1:0.2f} secs'.format(count,
          index_end-index_start))
    update_end = time.perf_counter()
    print('Updated {0} total movies in {1:.0f} secs...'.format(update_count,
          update_end-start))


start = time.perf_counter()
count = importMovies('../downloads/ml-20m/movies.csv', db)
import_end = time.perf_counter()

link_start = time.perf_counter()
updateMovieLinks('../downloads/ml-20m/links.csv', db, count)
link_end = time.perf_counter()
link_time = link_end-link_start

cast_start = time.perf_counter()
importCastCrew('../downloads/tmdb-5000-movie-dataset/tmdb_5000_credits.csv',
               db)
cast_end = time.perf_counter()


end = time.perf_counter()
print('Entire process took {0:.0f} secs'.format(end-start))
