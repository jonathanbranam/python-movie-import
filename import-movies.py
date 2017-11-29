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

debug = False

if debug:
    ML_MOVIES_FILENAME = '../downloads/test/test-ml-movies.csv'
    ML_LINKS_FILENAME = '../downloads/test/test-ml-links.csv'
    TMDB_MOVIES_FILENAME = \
        '../downloads/test/test-tmdb-movies.csv'
    TMDB_CREDITS_FILENAME = \
        '../downloads/test/test-tmdb-credits.csv'
else:
    ML_MOVIES_FILENAME = '../downloads/ml-20m/movies.csv'
    ML_LINKS_FILENAME = '../downloads/ml-20m/links.csv'
    TMDB_MOVIES_FILENAME = \
        '../downloads/tmdb-5000-movie-dataset/tmdb_5000_movies.csv'
    TMDB_CREDITS_FILENAME = \
        '../downloads/tmdb-5000-movie-dataset/tmdb_5000_credits.csv'


def importMovieLensMovies(file_name, db):
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
                'mlId': row['movieId'],
                'title': row['title'],
                'mlTitle': row['title'],
                'genres': genres,
                'mlGenres': genres,
                'cast': [],
                'crew': []
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
            movieList = []

    index_start = time.perf_counter()
    print('Indexing {0} movies...'.format(count))
    movies.create_index('mlId')
    movies.create_index('title')
    index_end = time.perf_counter()
    print('Indexed {0} movies in {1:0.2f} secs'.format(count,
          index_end-index_start))
    import_end = time.perf_counter()
    print('Imported {0} total movies in {1:.0f} secs...'.format(count,
          import_end-start))
    return count


def castCreateOrUpdate(member, movie, cast):
    lookup = cast.find_one({'tmdbId': member['id']})
    if lookup is None:
        return InsertOne({
            'tmdbId': member['id'],
            'name': member['name'],
            'gender': member['gender'],
            'roles': [{
                'movieId': movie['_id'],
                'title': movie['title'],
                'order': member['order'],
                'character': member['character'],
            }]
        })
    else:
        return UpdateOne(
            {'_id': lookup['_id']},
            {'$push': {
                'roles': {
                    'movieId': movie['_id'],
                    'title': movie['title'],
                    'order': member['order'],
                    'character': member['character'],
                }
            }}
        )


def mergeCastMovies(db):
    movies = db.movies
    cast = db.cast
    updateList = []
    update_count = 0
    count = 106257
    start = time.perf_counter()
    for member in cast.find():
        for role in member['roles']:
            movie = movies.find_one({'_id': role['movieId']})
            if movie is not None:
                update_count += 1
                updateList.append(UpdateOne(
                    {'_id': movie['_id']},
                    {'$push': {
                        'cast': {
                            'castId': member['_id'],
                            'name': member['name'],
                            'order': role['order'],
                            'character': role['character'],
                        }
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
        # print(updateList)
        movies.bulk_write(updateList, ordered=False)
        cur_time = time.perf_counter()
        print('Updated {0} ({1} / {2} in {3:.2f} secs) movies...'
              .format(len(updateList), update_count, count,
                      cur_time-start))
        updateList = []


def importCastCrew(file_name, db):
    db.drop_collection('cast')
    movies = db.movies
    cast = db.cast
    cast.create_index('tmdbId')
    cast.create_index('name')
    cast.create_index('roles.movieId')
    cast_count = 0
    missing_movies = 0
    start = time.perf_counter()
    with open(file_name) as castFile:
        castReader = csv.DictReader(castFile)
        # columns: movie_id,title,cast,crew
        castList = []
        for row in castReader:
            movie = movies.find_one({'tmdbId': row['movie_id']})
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


def updateMovieLensLinks(file_name, db, count):
    movies = db.movies
    update_count = 0
    start = time.perf_counter()
    with open(file_name) as linkFile:
        linkReader = csv.DictReader(linkFile)
        # columns: movieId,imdbId,tmdbId
        updateList = []
        for row in linkReader:
            movie = movies.find_one({'mlId': row['movieId']})
            if movie is not None:
                update_count += 1
                # Updating each movie once at a time is far too slow
                # movie['imdb_id'] = row['imdbId']
                # movie['tmdb_id'] = row['tmdbId']
                # movies.save(movie)
                updateList.append(UpdateOne(
                    {'_id': movie['_id']},
                    {'$set': {
                        'imdbId': row['imdbId'],
                        'tmdbId': row['tmdbId']
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
            updateList = []

    index_start = time.perf_counter()
    print('Indexing {0} movies...'.format(count))
    movies.create_index('tmdbId')
    movies.create_index('imdbId')
    index_end = time.perf_counter()
    print('Indexed {0} movies in {1:0.2f} secs'.format(count,
          index_end-index_start))
    update_end = time.perf_counter()
    print('Updated {0} total movies in {1:.0f} secs...'.format(update_count,
          update_end-start))


def parseMergeGenres(genres, tmdb_genres):
    tmdb_data = json.loads(tmdb_genres)
    tmdb_set = set()
    for genre in tmdb_data:
        tmdb_set.add(genre['name'])
    orig_set = set(genres)
    genres = list(orig_set | tmdb_set)
    return genres, list(tmdb_set)


def parseTmdbKeywords(tmdb_keywords):
    tmdb_data = json.loads(tmdb_keywords)
    keywords = []
    for kw in tmdb_data:
        keywords.append(kw['name'])
    return keywords


def parseFloat(value, default=0):
    result = default
    try:
        result = float(value)
    except ValueError:
        result = default
    return result


def parseInt(value, default=0):
    result = default
    try:
        result = int(value)
    except ValueError:
        if value.find('.') != -1:
            try:
                result = int(float(value))
            except ValueError:
                result = default
        else:
            result = default
    return result


def importTmdbMovieData(movie, row):
    genres = []
    if movie is not None:
        genres = movie['genres']
    genres, tmdb_genres = parseMergeGenres(genres, row['genres'])
    fields = {
        'budget': parseInt(row['budget']),
        'genres': genres,
        'tmdbGenres': tmdb_genres,
        'homepage': row['homepage'],
        'keywords': parseTmdbKeywords(row['keywords']),
        'originalLanguage': row['original_language'],
        'originalTitle': row['original_title'],
        'overview': row['overview'],
        'tmdbPopularity': parseFloat(row['popularity']),
        'productionCompanies': row['production_companies'],
        'productionCountries': row['production_countries'],
        'releaseDate': row['release_date'],
        'revenue': parseInt(row['revenue']),
        'runtime': parseInt(row['runtime']),
        'spokenLanguage': row['spoken_languages'],
        'status': row['status'],
        'tagline': row['tagline'],
        'tmdbTitle': row['title'],
        'tmdbVoteAverage': parseFloat(row['vote_average']),
        'tmdbVoteCount': parseInt(row['vote_count'])
    }
    if movie is None:
        fields['tmdbId'] = row['id']
        fields['title'] = fields['tmdbTitle']
        return InsertOne(fields)
    else:
        return UpdateOne(
            {'_id': movie['_id']},
            {'$set': fields}
        )


def importTmdbMovies(file_name, db, count):
    movies = db.movies
    update_count = 0
    new_movie_count = 0
    start = time.perf_counter()
    with open(file_name) as linkFile:
        linkReader = csv.DictReader(linkFile)
        updateList = []
        for row in linkReader:
            movie = movies.find_one({'tmdbId': row['id']})
            # columns: budget,genres,homepage,id,keywords,original_language,
            # original_title,overview,popularity,production_companies,
            # production_countries,release_date,revenue,runtime,
            # spoken_languages,status,tagline,title,vote_average,vote_count
            update = importTmdbMovieData(movie, row)
            updateList.append(update)
            if movie is not None:
                update_count += 1
            else:
                new_movie_count += 1

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
            updateList = []

    update_end = time.perf_counter()
    print('Updated {0} total movies in {1:.0f} secs...'.format(update_count,
          update_end-start))
    print('{0} movies in TMDB database not found.'.format(new_movie_count))


# ML_MOVIES_FILENAME
# ML_LINKS_FILENAME
# TMDB_MOVIES_FILENAME
# TMDB_CREDITS_FILENAME
start = time.perf_counter()
count = importMovieLensMovies(ML_MOVIES_FILENAME, db)
import_end = time.perf_counter()

link_start = time.perf_counter()
print('Updating Movie Lens links to TMDB and IMDB...')
updateMovieLensLinks(ML_LINKS_FILENAME, db, count)
link_end = time.perf_counter()
link_time = link_end-link_start

link_start = time.perf_counter()
print('Importing TMDB Movie data...')
importTmdbMovies(TMDB_MOVIES_FILENAME, db, count)
link_end = time.perf_counter()
link_time = link_end-link_start

cast_start = time.perf_counter()
print('Importing TMDB Cast data...')
importCastCrew(TMDB_CREDITS_FILENAME, db)
cast_end = time.perf_counter()

cast_merge_start = time.perf_counter()
print('Merge cast data to movies...')
mergeCastMovies(db)
cast_merge_end = time.perf_counter()


end = time.perf_counter()
print('Entire process took {0:.0f} secs'.format(end-start))
