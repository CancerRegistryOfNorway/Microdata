# Preparing data for the Microdata 2.0 project

This module provides a set of functions to read, process, validate, package, and encrypt CSV datasets and metadata files, in accordance with the requirements of the Microdata 2.0 project. The primary purpose is to handle CSV files and associated metadata, ensuring data integrity through validation checks and encryption for secure transmission. 

## Features

- **CSV File Validation**: Ensures that all rows in the CSV file have a consistent number of columns.
- **Metadata Extraction**: Extracts metadata filenames from CSV files.
- **Directory Management**: Prepares directories for processing and storing datasets.
- **Metadata Download**: Retrieves metadata from remote URLs in JSON format.
- **Dataset and Metadata Validation**: Validates both datasets and metadata files.
- **Packaging and Encryption**: Packages and encrypts datasets that successfully pass validation checks.

## Functions

### CSV File Processing

- **`check_csv_symmetry(file_path, delimiter=";", encoding="utf-8-sig")`**
  - Ensures that all rows in a CSV file have the same number of columns as the header.

- **`read_csv_file(input_csv_filename, delimiter=";", encoding="utf-8-sig")`**
  - Reads the input CSV file and returns it as a pandas DataFrame.

- **`extract_metadata_filenames(input_csv, output_metadata_file, columns_to_skip)`**
  - Extracts metadata filenames from the input CSV and writes them to a file, skipping specified columns.

- **`save_variable_to_csv(df, metadata_filenames, number_of_rows, input_directory_path)`**
  - Saves each variable from the CSV file as a separate file in the specified directory.

### Metadata Handling

- **`read_metadata_filenames(metadata_filenames_file, encoding="utf-8-sig")`**
  - Reads metadata filenames from a file and returns them as a list.

- **`download_metadata(metadata_filenames, url_without_metadata_parameter, input_directory_path)`**
  - Downloads metadata files in JSON format from the given URL and saves them to the appropriate directory.

### Validation

- **`validate_downloaded_metadata(metadata_filenames, input_directory_path)`**
  - Validates downloaded metadata files against predefined specifications.

- **`validate_created_dataset(metadata_filenames, input_directory_path)`**
  - Validates the created datasets using the metadata files.

### Packaging and Encryption

- **`package_and_encrypt_dataset(metadata_filenames, rsa_keys_dir, input_directory_path, output_directory_path)`**
  - Packages and encrypts the datasets that have passed the validation checks.
  - rsa_keys_dir should contain the public key in .pem format and must be named as "microdata_public_key.pem"

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/CancerRegistryOfNorway/Microdata.git
    ```

2. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Set environment variables:
    - `INPUT_VARIABLES_CSV_FILE`: Path to the input CSV file with real data.
    - `URL_WITHOUT_METADATA_PARAMETER`: Base URL for downloading metadata.
    - `INPUT_DIR`: Directory for storing processed CSV data.
    - `OUTPUT_DIR`: Directory for storing encrypted datasets.
    - `RSA_KEYS_DIR`: Directory containing a RSA public key in .pem form received from the Microdata team. 

## Usage

To run the main process, use the following command:

```bash
python microdata_csv_ssb_formatting.py
```

## Output
The path specified in OUTPUT_DIR contains the final packaged and encrypted file, with each validated dataset stored in a separate `.tar` file. 
If there are any validation errors, they are printed to the console.

If you have any questions, please reach out to nara[at]kreftregisteret.no. Replace [at] with @ while sending an email. 
