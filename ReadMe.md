# Python log file reader

Scenario:

1. Some other program is periodically appending lines to a log file.
2. Your program runs every few minutes and needs to process any new log lines added since the last run.

If it's just a single log file that keeps growing, use `log_file_reader.read(...)`.

If it's a log file that is rotated (e.g. by `logrotate`), use `log_file_reader.read_rotated(...)`.

## Using it

This is not published to PyPI yet.  Just take "log\_file\_reader.py" and use it.

## Developing

Make sure you have [Poetry](https://python-poetry.org/docs/#installation) installed.

1. Make sure dependencies are installed: `poetry install`
2. Activate local environment: `poetry shell`
3. Run tests: `pytest --cov=. --cov-branch --cov-report html log_file_reader_test.py`
    * Coverage report is generated to "htmlcov/index.html".
