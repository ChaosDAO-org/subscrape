# subscrape
A Python scraper for substrate chains

# Tasks
Open:
- rate limiting
- paging
- error handling
  - http response codes
- dump to csv


# Architecture

## General
We use the following methods in the projects:
- Logging: https://docs.python.org/3/howto/logging.html
- Async Operations: https://docs.python.org/3/library/asyncio-task.html

## SubscanWrapper
There is a class SubscanWrapper that encapsulates the logic around calling Subscan.
API: https://docs.api.subscan.io/
If you have a Subscan API key, you can put it in the main folder in a file called "subscan-key" and it will be applied to your calls.

