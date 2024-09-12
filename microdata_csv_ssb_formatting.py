# Author: Narasimha Raghavan
# Date: 2024-09-12
# Version: 1.0
"""
This module contains functions to read, process, create, and validate CSV and metadata 
files as per the requirements of the Mproject.

Functions included:
- check_csv_symmetry: Ensures CSV files have consistent columns across rows.
- read_csv_file: Reads a CSV file and returns it as a pandas DataFrame.
- read_metadata_filenames: Reads metadata filenames from a text file.
- prepare_directory: Prepares or creates a directory for file storage or validation.
- save_variable_to_csv: Saves CSV variables into separate files for further processing.
- download_metadata: Downloads metadata from a URL and saves it to the appropriate directory.
- validate_downloaded_metadata: Validates the downloaded metadata files.
- validate_created_dataset: Validates the created dataset against metadata specifications.
"""

import csv
import os
import time
import pandas as pd
import requests
from microdata_tools import (
    validate_dataset,
    validate_metadata,
)  # Updated import statement
from termcolor import colored


def check_csv_symmetry(file_path, delimiter=";", encoding="utf-8-sig"):
    """
    Checks if all rows in a CSV file have the same number of columns as the header.

    Args:
        file_path (str): The path to the CSV file.
        delimiter (str): The delimiter used in the CSV file. Default is ';'.
        encoding (str): The encoding of the CSV file. Default is 'utf-8-sig'.

    Returns:
        None: Prints a message whether all rows have the same number of columns
        or lists the rows with issues.
    """
    # Open the file and read lines
    with open(file_path, "r", encoding=encoding) as file:
        lines = file.readlines()

    # Split the first row (header) and check its length
    header = lines[0].strip().split(delimiter)
    header_length = len(header)
    print(f"Number of columns in the header: {header_length}")

    # Flag to track if all rows are aligned
    all_rows_aligned = True
    issues = []

    # Iterate through each row, check column count, and flag any issues
    for i, line in enumerate(lines, start=1):  # Start from the first row
        columns = line.strip().split(delimiter)
        if len(columns) != header_length:
            # If any row is not aligned, flag it
            all_rows_aligned = False
            issues.append((i, len(columns)))

    # Print results based on the checks
    if all_rows_aligned:
        print("All rows have the same number of columns.")
    else:
        print("The following rows have column count issues:")
        for issue in issues:
            print(f"Row {issue[0]} has {issue[1]} columns (Expected {header_length})")


def read_csv_file(input_csv_filename, delimiter=";", encoding="utf-8-sig"):
    """
    Reads the input CSV file and returns the DataFrame.
    Handles the Byte Order Mark (BOM) if present.

    Args:
        input_csv_filename (str): The path to the input CSV file.
        delimiter (str): The delimiter used in the CSV file. Default is ';'.
        encoding (str): The encoding of the CSV file. Default is 'utf-8-sig'.

    Returns:
        pd.DataFrame or None: Returns a DataFrame if successful, or None if an error occurs.
    """
    try:
        # Attempt to read the CSV file using the Python engine with potential spaces handled
        df = pd.read_csv(
            input_csv_filename,
            delimiter=delimiter,
            encoding=encoding,
            engine="python",
            skipinitialspace=True,
        )
        print("CSV file read successfully.")
        return df
    except FileNotFoundError:
        print(f"Error: File '{input_csv_filename}' not found.")
    except pd.errors.EmptyDataError:
        print(f"Error: File '{input_csv_filename}' is empty.")
    except pd.errors.ParserError:
        print(
            f"Error: Parsing error occurred while reading the file '{input_csv_filename}'."
        )
    except OSError as e:
        print(f"An unexpected I/O error occurred: {str(e)}")

    return None


def read_metadata_filenames(metadata_filenames_file, encoding="utf-8-sig"):
    """
    Reads the metadata filenames from a file and returns a list.
    Handles BOM if present in the file.
    """
    with open(metadata_filenames_file, "r", encoding=encoding) as file:
        return [line.strip().upper() for line in file.readlines() if line.strip()]


def prepare_directory(directory):
    """
    Prepares the directory for validation by creating it if it doesn't exist
    and returning its absolute path.
    """
    if os.path.exists(directory):
        os.chdir(directory)
        return os.path.abspath(os.getcwd())
    else:
        os.mkdir(directory)
        os.chdir(directory)
        return os.path.abspath(os.getcwd())


def save_variable_to_csv(df, metadata_filenames, number_of_rows, input_directory_path):
    """
    Converts the column names to lowercase and stores each variable in the column
    as a separate CSV file along with start, stop, and person_id.
    """
    for name_index, filename in enumerate(metadata_filenames):
        filename = filename.lower()
        metadata_filenames[name_index] = filename
        # Ensure the column exists in the DataFrame
        if filename not in df.columns:
            print(f"Warning: Column '{filename}' not found in DataFrame. Skipping.")
            continue
        if filename not in ["sidkrg", "start_time", "stop_time"]:
            # Convert the filename and directory name to uppercase
            filepath = filename.upper() + ".CSV"
            directory_name = filename.upper()

            if os.path.exists(directory_name):
                os.chdir(directory_name)
            else:
                os.mkdir(directory_name)
                os.chdir(directory_name)

            with open(filepath, "w", encoding="utf-8-sig") as csvfile:
                writer = csv.writer(csvfile, delimiter=";")
                # third column needs an empty string since five columns are required.
                for i in range(number_of_rows):
                    # Log data for debugging
                    row_data = [
                        df["sidkrg"].values[i],
                        df[filename].values[i],  # Use the correct column name
                        "",
                        df["start_time"].values[i],
                        df["stop_time"].values[i],
                    ]
                    writer.writerow(row_data)

            # Return to the original directory
            os.chdir(input_directory_path)


def download_metadata(
    metadata_filenames, url_without_metadata_parameter, input_directory_path
):
    """
    Downloads metadata in JSON format from the given URL and saves it to the appropriate directory.

    Args:
        metadata_filenames (list): A list of metadata filenames to be downloaded.
        url_without_metadata_parameter (str): The base URL to download metadata from.
        input_directory_path (str): The directory where metadata will be saved.
    """
    for _, filename in enumerate(metadata_filenames):
        filename = filename.lower()

        # Skip certain filenames
        if filename not in ["sidkrg", "start_time", "stop_time"]:
            metadata_filename = filename.upper() + ".JSON"
            directory_name = os.path.join(input_directory_path, filename.upper())

            # Full URL to retrieve metadata
            url_with_metadata_parameter = url_without_metadata_parameter + filename

            try:
                # Fetch the metadata with a 30-second timeout
                r = requests.get(url_with_metadata_parameter, timeout=30)
                r.raise_for_status()  # Raise an error if the request failed

                # Create directory if it doesn't exist
                if not os.path.exists(directory_name):
                    os.makedirs(directory_name)

                # Save the metadata file in the correct directory
                file_path = os.path.join(directory_name, metadata_filename)
                with open(file_path, "wb") as f:
                    f.write(r.content)

                print(
                    f"Successfully retrieved and saved {metadata_filename} to {directory_name}"
                )

            except requests.exceptions.Timeout:
                print(f"Request timed out for URL: {url_with_metadata_parameter}")

            except requests.exceptions.RequestException as e:
                print(
                    f"Failed to retrieve metadata from {url_with_metadata_parameter}: {e}"
                )


def validate_downloaded_metadata(metadata_filenames, input_directory_path):
    """
    Validates the downloaded metadata files against the specified directory.

    Args:
        metadata_filenames (list): A list of metadata filenames to be validated.
        input_directory_path (str): The directory where the metadata files are located.

    Returns:
        list: A list of valid metadata filenames that passed the validation process.
    """
    valid_metadata = []
    for metadata_filename in metadata_filenames:
        # Perform validation
        validation_errors = validate_metadata(
            metadata_filename.upper(), input_directory=input_directory_path
        )
        # Check if there are any validation errors
        if not validation_errors:
            print(colored(f"Metadata for {metadata_filename} looks good", "green"))
            valid_metadata.append(metadata_filename)
        else:
            print(colored(f"Invalid metadata for {metadata_filename} :(", "red"))
        # Print validation errors, if any
        for error in validation_errors:
            print(f"{metadata_filename}: {error}")
    return valid_metadata


def validate_created_dataset(metadata_filenames, input_directory_path):
    """
    Validates the created dataset files against the specified directory.

    Args:
        metadata_filenames (list): A list of metadata filenames whose
        corresponding datasets need validation.
        input_directory_path (str): The directory where the dataset files are located.

    Returns:
        None: Prints the validation results for each dataset.
    """
    for metadata_filename in metadata_filenames:
        # Perform validation
        validation_errors = validate_dataset(
            metadata_filename.upper(), input_directory=input_directory_path
        )
        # Check if there are any validation errors
        if not validation_errors:
            print(colored(f"Dataset for {metadata_filename} looks good", "blue"))
        else:
            print(colored(f"Dataset for {metadata_filename} :(", "red"))
        # Print validation errors, if any
        for error in validation_errors:
            print(f"{metadata_filename}: {error}")


def main():
    """
    Main function that coordinates the reading, processing, saving, and validation
    of CSV and metadata files.

    The function performs the following tasks:
    1. Checks the symmetry of the CSV file (ensuring all rows have the same number of columns).
    2. Reads the CSV file and metadata filenames.
    3. Saves each variable from the CSV into a separate CSV file.
    4. Downloads metadata in JSON format from a specified URL.
    5. Validates the downloaded metadata files.
    6. Validates the created dataset using the metadata.

    Returns:
        None: Prints the process steps and the total execution time.
    """
    print("The execution has begun..")
    start = time.perf_counter()

    # Input two CSV files: data and metadata
    input_csv_filename = "Microdata_real_data.csv"
    metadata_filenames_file = "MIcrodata_metadata_variables.csv"

    # Input the URL for retrieving metadata
    url_without_metadata_parameter = (
        "https://metadata-test.kreftregisteret.no/rest/v1/metadata/variable/microdata/"
    )

    # Check the symmetry of the CSV file:
    # all rows of the data should have the same number of columns
    check_csv_symmetry(input_csv_filename)

    # Read the CSV file and metadata filenames
    df = read_csv_file(input_csv_filename)
    metadata_filenames = read_metadata_filenames(metadata_filenames_file)

    # Get the total number of rows in the CSV file
    number_of_rows = df.shape[0]
    print("Number of rows in the CSV file: ", number_of_rows)

    # Prepare the directory for validation
    input_directory_path = prepare_directory("input_directory")

    # Save each variable to a CSV file
    save_variable_to_csv(df, metadata_filenames, number_of_rows, input_directory_path)

    # Download metadata in JSON format
    download_metadata(
        metadata_filenames, url_without_metadata_parameter, input_directory_path
    )

    # Validate the downloaded metadata
    valid_metadata_filenames = validate_downloaded_metadata(
        metadata_filenames, input_directory_path
    )

    # Validate the created dataset and metadata
    validate_created_dataset(valid_metadata_filenames, input_directory_path)

    print(f"Total Duration: {time.perf_counter() - start}")


if __name__ == "__main__":
    main()
