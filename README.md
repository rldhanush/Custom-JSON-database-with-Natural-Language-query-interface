# Custom JSON database with Natural Language query interface

Welcome to the JSON Database project repository! This project implements a custom NoSQL database system designed to manage JSON-formatted data efficiently. It includes a user-friendly natural language query interface, enabling seamless interaction with the database.

## Overview
The JSON Database system provides:

* Efficient Data Management: Chunk-based processing for large JSON datasets, avoiding full memory load and ensuring scalability.
* Natural Language Interface: Regex-based matching for interpreting natural language queries, offering user-friendly interaction.
*Robust JSON Handling: Built to handle various data-driven applications with a focus on reliability and performance.

## Features
* Data Storage Strategy: Newline-Delimited JSON (NDJSON) format for efficient CRUD operations.
* Data Access Strategy: Byte-Offset Indexing and Hash Indexes for rapid data retrieval.
* Query Processing: Streamlined for memory efficiency, supporting operations like projection, filtering, joining, grouping, aggregation, and ordering.
* Query Parsing: Interprets natural language commands for insertion, deletion, and modification operations.
* Command-Line Interface (CLI): Utilizes cmd2 Python Package for interactive querying and modification of data.
* Scalability and Performance: Implements strategies for efficient join operations and optimized query execution.

## Installation and Usage
Follow these steps to set up and run the JSON Database system locally:

### Prerequisites
* Python 3.x installed on your system.
### Installation
** Clone the repository to your local machine.
** Navigate to the project directory.
## Usage
* Open your terminal.
* Run the following commands:
```bash
# Check if python virtual environment is installed, if not install
pip install virtualenv

# Create a new instance of virtual environment in your project directory
python -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Set up required packages for implementation of project
pip install -r requirements.txt

# Verify installation
pip freeze
```
* Start the shell script:
``` bash
./start_db.sh
```
* Follow the instructions to interact with the database system using natural language commands.

### Directory Structure
```sql
JSONDatabase - main root directory 
    src - contains all source code, partitioned into subdirectories 
        - data_access : code to execute queries
        - data_storage : code to create and store the database
        - query_processing : code to parse queries
    dataset - available datasets to test
    databases - created databases using the project
    start_db.sh - shell script to start the database system
```

### Sample Queries
* Start with exploring the avaliable queries using 'help' command inside the JSONDatabase Shell process
* Create a document database
    * Creating a doucment database needs two inputs - Location path of JSON data and nested primary key to be used.
    ```python
    > create_document [Name of document]
    # On enter - user is prompted to enter the Primary key for the document in its nested format; eg: artists.name -> artists : document name and name: pk (this can be nested - within name.id )
    ```
* To execute any query - we need to start the execution process. Two steps: 
    * Initate the usage of a document on which you want to execute queries
      ```python
      > use_document [Document Name]
      ```
    * Initiate the execution process
      ```python
      > execute_query
      # On enter - we get a prompt to enter query
      # Usually for most queries - chunk size to be used by process engine is requested to the user - to implement data management
      ```
* Projection:
  ```python
  > Show all tracks with artists=Adele and popularity>=75
  > Find name, artists, release_date, popularity, duration_sec in records where release_date>=2010 and duration_sec>=250.0 and popularity>80
  ```
* Insertion:
  ```python
  # Structure:  Add new [ENTITY] with [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE]
  > Add new artist with name=DSCI551 followers=300 popularity=90
  >
  ```
* Update:
  ```python
  # Structure:  Modify [ENTITY/record] with [ATTRIBUTE]=[VALUE] set [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE]
  # Use only - 'With' (usage of 'which'/'where' is invalid)
  > Modify records with name=DSCI551 set popularity=75, followers=250, location=SLH100
  ```
* Delete:
  ```python
  # Structure:  Delete [ENTITY/record] with [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE]
  # Use only - 'With' (usage of 'which'/'where' is invalid)
  > Delete records with popularity=55 and genres=other
  ```
* Group by Aggregations:
  ```python
  > Display maximum(duration_sec) of tracks where release_date>2018 and popularity>80 categorize_by artists
  > Find count(*), total_sum(GNP), maximum(GNP) of records categorize_by Continent
  ```
* Sort:
  ```python
  > Find Name, GNP, Region in records with Continent=Asia sort_by GNP asc
  > Show Name, Population, GNP, LifeExpectancy of countries where GNP>300000 and Continent!=Europe sort_by LifeExpectancy desc
  ```
* Simple inner Join
  ```python
  > Join country and city where country.country_code=city.CountryCode display country.Name, city.Name, city.Population country.Continent, country.Population
  > Join SpotifyArtist and SpotifyTracks where SpotifyArtists.name=SpotifyTracks.artists display SpotifyArtist.name, SpotifyArtist.genres, SpotifyTracks.name, SpotifyTracks.popularity
  ```

## Future Scope
We aim to enhance the project with advanced Natural Language Processing (NLP), performance optimizations, and feature expansions. Your feedback and contributions will help shape the future of this project.

## Project Report
For detailed insights into the project implementation, architecture, challenges faced, and future scope, refer to the project report provided in the [Google Drive link](https://drive.google.com/file/d/1fn08NjbYqfZDaCL9zz3Zn2BcF-DgC0Lf/view?usp=sharing). 

## Contributions and Support
Contributions to the project are welcome! Please refer to the guidelines outlined in the CONTRIBUTING.md file. For support or queries, feel free to open an issue or reach out to the project maintainer.

## Credits
* Kaggle dataset - [Spotify Million](https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?resource=download)
* Generative AI - ChatGPT



