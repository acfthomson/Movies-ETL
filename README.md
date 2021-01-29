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
    
    Wiki_movies_df contained 193 columns
    
  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
