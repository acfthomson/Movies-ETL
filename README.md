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

In order to catch duplicates and errors, a try-except block was incorporated into the code.
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
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
