![](https://web.superquery.io/wp-content/uploads/2019/03/sq-logotype@1x.svg)

# Python API for superQuery

Python API library for superQuery

# Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

* Python >= 3.1

### Installing

```
pip install superQuery
```

# Authentication
* Go to superquery.io and log in/sign up
* In the left side-bar, scroll down and click on "Integrations"
* Scroll down until you see "Python" and click "Connect"
* Note the username and password

# The basic flow
* Get your autentication details
* Import the superQuery library: 

``` 
from superQuery import superQuery
``` 

* Create a superQuery client: 
``` 
client = superQuery.Client()
```

* Decide what SQL statement you'd like to run: 
``` 
QUERY = """SELECT name FROM `bigquery-public-data.usa_names.usa_1910_current` LIMIT 10"""
```

* Get your results generator: 
```
query_job = client.query(QUERY)
rows = query_job.result()
```

* Get your results by iteration (**Option A**)
```
for row in rows:
    print(row.name)
```

* Get your results by iteration and store to a Pandas dataframe (**Option B**)
```
import pandas as pd
df = pd.DataFrame(data=[x.to_dict() for x in rows])
```

# Examples
## Running `examples/start.ipynb` in Google Colab
* [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/superquery/superPy/blob/master/examples/start.ipynb)
* Update the credentials in the notebook by following the steps above under "Get your authentication details"
* Run it!

## Running `examples/start.ipynb` in Jupyter notebook
* Launch Jupyter locally with `jupyter notebook`
* Download `examples/start.ipynb` to your local machine and open it from Jupyter
* Update the credentials in the notebook by following the steps above under "Get your authentication details"
* Run it!


## Running `examples/start.py`
* First, set these two variables in your local environment:
  - SUPERQUERY_USERNAME=xxxxxx
  - SUPERQUERY_PASSWORD=xxxxxx
* Enter your projectId into this line:

```
client.set_project("yourprojectid")
```

* Alternatively: If you prefer to use your username/password combination directly for each query, then inside  [`start.py`](https://github.com/superquery/superPy/blob/master/examples/start.py) enter your details obtained from the superquery.io web interface where it shows `xxxxxxx` below

```
query_job = client.query(
    "SELECT field FROM `projectId.datasetId.tableID` WHERE _PARTITIONTIME = \"20xx-xx-xx\"", 
    username="xxxxxxxxx",
    password="xxxxxxxxx",
    project_id=None) # If you don't specify a project_id, your default project will be selected
```

* Now run
```
python3 examples/start_here.py
```

## Tested With

* [Python3.7.3](https://www.python.org/downloads/release/python-373/) - Python version

## Authors

* **Eben du Toit** - *v1.5* - [ebendutoit](https://github.com/ebendutoit)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* The awesome people at superQuery
