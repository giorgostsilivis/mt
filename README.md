#  Assignment: MarineTraffic File Tool


### Description

- Scheduled job parsing files every 3 seconds.
- Importing csv file datasets into an in memory database (3 tables).
- Exporting a joined view to an output csv file.

## Technologies

This project uses a number of open source projects to work properly:

- [Python] - Programming Language
- [Pandas] - Data analysis and manipulation tool
- [Sqlite] - Demo in memory database

##### First, clone the following git [public repository][dill] in a folder of your preference. Then, proceed to following steps.

## Installation

Application requires [Python](https://www.python.org/) v3.8 and pip3 to run on Linux Ubuntu. Also, install virtualenv to create a Python virtual environment.
```sh
sudo pip3 install virtualenv
```

Create and activate virtual environment.
```sh
python3 -m venv mt
source mt/bin/activate
```

Install the dependencies

```sh
cd mt
pip install -r requirements.txt
```


## Run

```sh
python3 main.py
```

### Demo use
>Move all csv files from mt/demo_files to mt/input.
Files will get parsed and one output file will get generated in mt/output folder. Finally, input files will get archived into mt/processed folder while our demo database will have already stored ingested data.
>app.log auto-generated log file provides essential application's logs.




### Running on background

```sh
cd mt
nohup python3 /main.py &
```
Confirm your process is running
```sh
ps ax | grep main.py
```
To kill background running process:
```sh
pkill -f test.py
```



   [dill]: https://github.com/giorgostsilivis/mt
   [Sqlite]: <https://www.sqlitetutorial.net>
   [Pandas]: <https://pandas.pydata.org>
   [Python]: <https://www.python.org>
