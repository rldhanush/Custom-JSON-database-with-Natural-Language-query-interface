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

