# Worst Movie API

Code to get the gaps between to wins in Worst Movie from 
Golden Raspberry Awards

The solution uses Flask and Flask RESTful to implement the API. Pandas was also used to simplify some process in the code.
SQLite was used as an embedded database as it not necessary any aditional installation

## Installation

Python virtual environment was used to develop the solution. 

For Windows 10:

```bash
python -m venv venv
venv\Scripts\activate
```

For Linux

```bash
python3 -m venv venv
source venv/bin/activate
```

Use the package manager pip to install the requirements available in requirements.txt. With the virtual environment activated:

```bash
pip install -r requirements.txt (Windows)
pip3 install -r requirements.txt (Linux)
```

The requirements needed to execute are listed below:

- Flask==2.1.2
- Flask-RESTful==0.3.9
- pandas==1.4.1

To run the project:

```bash
python -m flask run (Windows)
python3 -m flask run (Linux)
```

## Usage

To get information from the API, Postman can be used with the given URL available in the description document.

**Method:** GET
- /worstmovie/gap/find
- /worstmovie/gap/find/\<path> (optional parameter)

Additionally it can be used a optinal parameter with the name of other csv file.
If none were passed the code will use de default path 'movielist.csv'.
The file passed must be in the root folder otherwise will raise an exception. 

## Testing
To run the tests just execute the fallowing command in the terminal:

```bash
python -m unittest tests/tests.py (Windows)
python3 -m unittest tests/tests.py (Linux)
```

These tests consist in validate the return content in cases of success. 
It also validate if the expected keys are present in response