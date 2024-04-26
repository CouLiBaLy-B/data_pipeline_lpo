**Data Pipeline with Astronomer: ETL for Biodiv-Sport API**

**Overview**

This data pipeline project uses Astronomer to extract, transform, and load data from the Biodiv-Sport API into BigQuery. The pipeline is designed to run daily and fetches data from the API, applies various transformations, and loads the data into a BigQuery dataset.

**Components**

* **Data Extraction**: The pipeline uses the `requests` library to fetch data from the Biodiv-Sport API.
* **Data Transformation**: The pipeline applies various transformations to the data, including:
	+ Removing HTML tags from text fields
	+ Converting dates to a standard format
	+ Extracting location information from coordinates
	+ Adding month names to the data
	+ Exploding arrays into separate rows
	+ Converting data types
* **Data Loading**: The pipeline uses the `pandas_gbq` library to load the transformed data into a BigQuery dataset.

**Configuration**

The pipeline uses environment variables to configure the API URL, project ID, dataset name, and service account credentials. These variables are stored in a `.env` file and loaded using the `dotenv` library.

**Schedule**

The pipeline is scheduled to run daily using Astronomer's scheduling feature.

**Dependencies**

The pipeline depends on the following libraries:

* `requests`
* `BeautifulSoup`
* `pandas`
* `pandas_gbq`
* `geopy`
* `dotenv`

**Service Account Credentials**

The pipeline uses a service account credentials file to authenticate with BigQuery. This file should be stored in a secure location, such as a secrets manager.

**BigQuery Dataset**

The pipeline loads data into a BigQuery dataset specified by the `DATASET_NAME` environment variable.

**Troubleshooting**

If you encounter any issues with the pipeline, check the Astronomer logs for errors and debug messages. You can also test individual tasks using the Astronomer CLI.
