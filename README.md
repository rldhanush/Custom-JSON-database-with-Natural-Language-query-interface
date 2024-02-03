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
  >
  >
  ```
* Insertion:
  ```python
  >
  >
  ```
* Update:
  ```python
  >
  >
  ```
* Delete:
  ```python
  >
  >
  ```
* Group by Aggregations:
  ```python
  >
  >
  ```
* Sort:
  ```python
  >
  >
  ```
* Simple inner Join
  ```python
  >
  >
  ```
  




