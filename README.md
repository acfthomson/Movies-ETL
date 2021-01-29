# Movies-ETL

## Overview
Fictional company Amazing Prime needs to create an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables. Code will be refactored to create one function that takes in the three files and performs the ETL process by adding the data to a PostgreSQL database. 

## Resources
Software
- Python v3.x
  - Dependencies:
    - pandas
    - json
    - numpy
    - re
    - sqlalchemy
    - psychopg2
    - time
 - PostgreSQL
    - pgAdmin
 - Files:
    - [movies_metadata.csv](https://github.com/acfthomson/Movies-ETL/tree/main/Resources)
    - [ratings_small.csv](https://github.com/acfthomson/Movies-ETL/tree/main/Resources)
    - [wikipedia-movies.json](https://github.com/acfthomson/Movies-ETL/tree/main/Resources)

## Results
The first part of the project required us to use Python to read in [movies_metadata.csv](https://github.com/acfthomson/Movies-ETL/tree/main/Resources), [ratings_small.csv](https://github.com/acfthomson/Movies-ETL/tree/main/Resources), and [wikipedia-movies.json](https://github.com/acfthomson/Movies-ETL/tree/main/Resources).

A function was created that took in the three files and turned them in DataFrames:
```python
# Create a function that takes in three arguments;
# Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle)
def extract_transform_load():
    
    # Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv')
    ratings = pd.read_csv(f'{file_dir}ratings.csv')

    # Open the read the Wikipedia data JSON file
    with open(f'{file_dir}wikipedia-movies.json', mode='r') as file:
        wiki_movies = json.load(file)
    
    # Read in the raw wiki movie data as a Pandas DataFrame
    wiki_movies_df = pd.DataFrame(wiki_movies)
    
    # Return the three DataFrames
    return wiki_movies_df, kaggle_metadata, ratings
    ```

The second part of the project required Python, Pandas, the ETL process, and code refactoring.  Data from the Wikipedia dataset was extracted and transformed so that it could be merged with the Kaggle dataset.

In order to catch duplicates and errors, a try-except block was incorporated into the code.  Regular expressions and lamda functions were also used.
```python
# 6. Write a try-except block to catch errors while extracting the IMDb ID using a regex string and
    # dropping any imdb_id duplicates
    # If there is an error, capture and print the exception
    try:
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
        print(wiki_movies_df)
        
    except:

        #  7. Write a list comprehension to keep the columns that don't have null values from wiki_movies_df
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() 
                                < len(wiki_movies_df) * 0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
        
        # 8. Create a variable that will hold the non-null values from the “Box office” column
        box_office = wiki_movies_df['Box office'].dropna()

        # 9. Convert the box office data created in Step 8 to string values using the lambda and join 
        # functions
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

        # 10. Write a regex to match the six elements of "form_one" of the box office data
        form_one = r'\$\d+\.?\d*\s*[mb]illion'
        
        # 11. Write a regular expression to match the three elements of "form_two" of the box office data
        form_two = r'\$\d{1,3}(?:,\d{3})+'

        # 12. Add the parse_dollars function
        def parse_dollars(s):
            
        # If s is not a string, return NaN
            if type(s) != str:
                return np.nan

        # If input is of the form $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # Remove dollar sign and " million"
                s = re.sub('\$|\s|[a-zA-Z]','', s)

            # Convert to float and multiply by a million
                value = float(s) * 10**6

            # Return value
                return value

        # If input is of the form $###.# billion
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # Remove dollar sign and " billion"
                s = re.sub('\$|\s|[a-zA-Z]','', s)

            # Convert to float and multiply by a billion
                value = float(s) * 10**9

            # Return value
                return value

        # If input is of the form $###,###,###
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # Remove dollar sign and commas
                s = re.sub('\$|,','', s)

            # Convert to float
                value = float(s)

            # Return value
                return value

        # Otherwise, return NaN
            else:
                return np.nan
```

The wiki_movies DataFrame was whittled down from 193 columns to 21:
```python
# 21. Check that wiki_movies_df DataFrame columns are correct. 
wiki_movies_df.columns.to_list()
['url',
 'year',
 'imdb_link',
 'title',
 'Based on',
 'Starring',
 'Cinematography',
 'Release date',
 'Running time',
 'Country',
 'Language',
 'Budget',
 'Box office',
 'Director',
 'Distributor',
 'Editor(s)',
 'Composer(s)',
 'Producer(s)',
 'Production company(s)',
 'Writer(s)',
 'box_office']
```

The third part of the project again required Python, Pandas, the ETL process, and code refactoring.  Data from the Kaggle metadata and MovieLens rating data was extracted and transformed so that it could be merged with the Wikipedia dataset.

A try-except block that utilized regular expressions and labda functions was also utilized to catch duplicates and errors.

The Kaggle dataset was reduced from 24 columns to 23 


The MovieLens rating data and Kaggle dataset were merged with Wikipedia dataset with the following code:
```python
 movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') 
                                         & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    
    # 4. Drop unnecessary columns from the merged DataFrame
    movies_df.drop(columns=['title_wiki','Language','Production company(s)'], inplace=True)

    # 5. Add in the function to fill in the missing Kaggle data
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] 
                                     == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)
    
    # 6. Call the function in Step 5 with the DataFrame and columns as the arguments
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # 7. Filter the movies DataFrame for specific columns
    for col in movies_df.columns:
        lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
        value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
        num_values = len(value_counts)
        if num_values == 1:
            print(col)
    movies_df['video'].value_counts(dropna=False)
    
    # 8. Rename the columns in the movies DataFrame
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline',
                                  'belongs_to_collection','url','imdb_link','runtime','budget_kaggle',
                                  'revenue','release_date_kaggle','popularity','vote_average','vote_count',
                                  'genres','original_language','overview','spoken_languages','Country',
                                  'production_companies','production_countries','Distributor','Producer(s)',
                                  'Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)',
                                  'Based on'
                                  ]]
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)

    # 9. Transform and merge the ratings DataFrame
    rating_counts = ratings.groupby(['movieId','rating'], 
                                    as_index=False).count() \.rename({'userId':'count'}, axis=1)
    rating_counts = ratings.groupby(['movieId','rating'], 
                                    as_index=False).count() \.rename({'userId':'count'}, axis=1) \
                                    .pivot(index='movieId',columns='rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', 
                                      right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    return wiki_movies_df, movies_with_ratings_df, movies_df
```

Lastly, Python, Pandas, the ETL process, code refactoring, and PostgreSQL to add the movies_df DataFrame and MovieLens rating CSV data to a SQL database. Connecting to a PostgreSQL database and adding the data was accomplished with the following code:
```
# Connect to PostgrSQL database and add movies_df to SQL database
    # Create a connection string
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5433/movie_data"
    
    # Create the database engine
    engine = create_engine(db_string)
    
    # Create SQL table
    movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    
    # Remove the ratings table from the database, if needed
    # Open a connection
    connection = engine.raw_connection()
    
    # Create a cursor object using the cursor() method
    cursor = connection.cursor()
    
    # Drop the ratings table if it already exists
    cursor.execute('DROP TABLE IF EXISTS ratings')
    
    # Commit database changes
    connection.commit()
    
    # Close the connection
    connection.close()
    
    # Create file path to directory and variable for the three files
    file_dir = 'C:/Users/acfth/Desktop/Data_Course/Modules/Module 8/Movies-ETL/Resources/'
    
    # Create a variable for the number of rows imported
    rows_imported = 0
    
    # Get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
        
        # Print out the range of rows being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        
        # Increment the number of imported rows by the chunksize
        rows_imported += len(data)

        # Add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')

extract_transform_load()
importing rows 0 to 1000000...Done. 65.0890896320343 total seconds elapsed
importing rows 1000000 to 2000000...Done. 128.2017457485199 total seconds elapsed
importing rows 2000000 to 3000000...Done. 193.19100856781006 total seconds elapsed
importing rows 3000000 to 4000000...Done. 259.1230351924896 total seconds elapsed
importing rows 4000000 to 5000000...Done. 324.4428024291992 total seconds elapsed
importing rows 5000000 to 6000000...Done. 392.6175937652588 total seconds elapsed
importing rows 6000000 to 7000000...Done. 458.79659605026245 total seconds elapsed
importing rows 7000000 to 8000000...Done. 521.8520712852478 total seconds elapsed
importing rows 8000000 to 9000000...Done. 585.017986536026 total seconds elapsed
importing rows 9000000 to 10000000...Done. 647.7974967956543 total seconds elapsed
importing rows 10000000 to 11000000...Done. 710.4469103813171 total seconds elapsed
importing rows 11000000 to 12000000...Done. 772.9878420829773 total seconds elapsed
importing rows 12000000 to 13000000...Done. 835.8154730796814 total seconds elapsed
importing rows 13000000 to 14000000...Done. 898.184454202652 total seconds elapsed
importing rows 14000000 to 15000000...Done. 960.9244728088379 total seconds elapsed
importing rows 15000000 to 16000000...Done. 1023.7157187461853 total seconds elapsed
importing rows 16000000 to 17000000...Done. 1086.3480575084686 total seconds elapsed
importing rows 17000000 to 18000000...Done. 1149.2522373199463 total seconds elapsed
importing rows 18000000 to 19000000...Done. 1211.7999548912048 total seconds elapsed
importing rows 19000000 to 20000000...Done. 1274.2821216583252 total seconds elapsed
importing rows 20000000 to 21000000...Done. 1337.0692598819733 total seconds elapsed
importing rows 21000000 to 22000000...Done. 1399.6721558570862 total seconds elapsed
importing rows 22000000 to 23000000...Done. 1462.0458884239197 total seconds elapsed
importing rows 23000000 to 24000000...Done. 1524.5053746700287 total seconds elapsed
importing rows 24000000 to 25000000...Done. 1587.3593175411224 total seconds elapsed
importing rows 25000000 to 26000000...Done. 1650.080182313919 total seconds elapsed
importing rows 26000000 to 26024289...Done. 1651.5680515766144 total seconds elapsed
```
