# PySpy Backend

[![Maintainability](https://api.codeclimate.com/v1/badges/4b0009e04d349069dd4a/maintainability)](https://codeclimate.com/github/jhmartin/PySpy-backend/maintainability)

This is the intel generation code for the EVE Online Intel Tool [PySpy](https://github.com/jhmartin/PySpy).

Data is fetched from https://data.everef.net/killmails/, which is a dump of the full killmails.

Summaries are generated from a local SQLite instance then pushed to DynamoDB for consumption by the server.
