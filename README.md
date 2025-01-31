# ast-revamp
A successor to the Automated Status Tool.



## Requirements

* [Poetry](https://python-poetry.org/docs/)
* [Python 3.12](https://www.python.org/)

Packages and dependencies are managed by Poetry.

## Setup

To install `poetry` on Windows, run the following commands in PowerShell:\
```
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -

[Environment]::SetEnvironmentVariable("Path", [Environment]::GetEnvironmentVariable("Path", "User") + ";{poetry_home_bin}", "User")

```
After running these commands, restart your powershell session.


To set up the environment for the script, run open the project directory in a terminal and run:

```
poetry install
```
This will install all the required dependencies into a virtual environment.

## Running


```
poetry run python <optional_script_name.py>

```