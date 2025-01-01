# PySpy Backend

[![Maintainability](https://api.codeclimate.com/v1/badges/4b0009e04d349069dd4a/maintainability)](https://codeclimate.com/github/jhmartin/PySpy-backend/maintainability)

This is the intel generation code for the EVE Online Intel Tool [PySpy](https://github.com/jhmartin/PySpy).

Relies on <https://github.com/jhmartin/PySpy-WebTerraform>.

Data is fetched from <https://data.everef.net/killmails/>, which is a batch dump of the full killmails available from zKill.

Summaries are generated from a local SQLite instance then pushed to DynamoDB for consumption by the server.

Processing a day takes about 2 hours, though that is with sqlite being hosted remotely on a NAS and DynamoDB being remote as well.

Quarterly backups of sqlite and dynamodb are available at s3://requesterpaysfiles/pyspy so forks won't have to start their datastore from scratch. Since I don't want to pay for 'the internet' to download a 40GB artifact, it is configured that you must have an AWS account and pay the retrieval and egress charges -- in any one instances it is just a few cents, but it adds up for me if everyone decides to do it.

* dayfetch.py - Fetches killmails and updates the DynamoDB table with the result
* shipnames.py - Fetches a mapping of shipnames from ESI and uploads to Cloudfront. Avoids clients beating up ESI for the same data since it changes rarely.
