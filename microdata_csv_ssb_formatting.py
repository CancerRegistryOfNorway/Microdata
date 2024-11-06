# Author: Narasimha Raghavan
# Date: 2024-11-06
# Version: 3.0
"""
This module contains functions to read, process, create, and validate CSV and metadata 
files as per the requirements of the Microdata 2.0 project.

Functions included:
- check_csv_symmetry: Ensures CSV files have consistent columns across rows.
- read_csv_file: Reads a CSV file and returns it as a pandas DataFrame.
- extract_metadata_filenames: Extracts metadata filenames from the input CSV file.
- read_metadata_filenames: Reads metadata filenames from a text file.
- prepare_directory: Prepares or creates a directory for file storage or validation.
- save_variable_to_csv: Saves CSV variables into separate files for further processing.
- download_metadata: Downloads metadata from a URL and saves it to the appropriate directory.
- validate_downloaded_metadata: Validates the downloaded metadata files.
- validate_created_dataset: Validates the created dataset against metadata specifications.
- package_and_encrypt_dataset: Packages and encrypts datasets that 
successfully pass validation checks of Microdata tools."""

import csv
import os
import time
from pathlib import Path
from datetime import date
import pandas as pd
import requests
from microdata_tools import validate_dataset, validate_metadata, package_dataset
from termcolor import colored
from prettytable import PrettyTable


# Define a timing decorator
def timed(func):
    """
    A decorator that records the time taken by a function to execute.

    Args:
        func (callable): The function to be timed.

    Returns:
        callable: Wrapped function that returns the original result and execution time.
    """

    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()  # Start timing
        result = func(*args, **kwargs)  # Execute the function
        elapsed_time = time.perf_counter() - start_time  # Calculate elapsed time
        return result, elapsed_time  # Return the result and elapsed time

    return wrapper


@timed
def check_csv_symmetry(file_path, delimiter=";", encoding="utf-8-sig"):
    """
    Checks if all rows in a CSV file have the same number of columns as the header.

    Args:
        file_path (str): The path to the CSV file.
        delimiter (str): The delimiter used in the CSV file.
        encoding (str): The encoding of the CSV file.

    Returns:
        None: Prints a message indicating any column count issues.
    """
    with open(file_path, "r", encoding=encoding) as file:
        lines = file.readlines()

    header = lines[0].strip().split(delimiter)
    header_length = len(header)
    print(f"Number of columns in the header: {header_length}")

    all_rows_aligned = True
    issues = []
    for i, line in enumerate(lines, start=1):
        columns = line.strip().split(delimiter)
        if len(columns) != header_length:
            all_rows_aligned = False
            issues.append((i, len(columns)))

    if all_rows_aligned:
        print("All rows have the same number of columns.")
    else:
        print("The following rows have column count issues:")
        for issue in issues:
            print(f"Row {issue[0]} has {issue[1]} columns (Expected {header_length})")

    return None  # Explicitly return None to ensure consistent unpacking


@timed
def read_csv_file(input_csv_filename, delimiter=";", encoding="utf-8-sig"):
    """
    Reads the input CSV file and returns a DataFrame.

    Args:
        input_csv_filename (str): Path to the CSV file.
        delimiter (str): Delimiter used in the CSV file.
        encoding (str): Encoding of the CSV file.

    Returns:
        pd.DataFrame: The data as a DataFrame, or None if there's an error.
    """
    try:
        df = pd.read_csv(
            input_csv_filename,
            delimiter=delimiter,
            encoding=encoding,
            engine="python",
            skipinitialspace=True,
        )
        print("CSV file read successfully.")
        return df
    except Exception as e:
        print(f"Error occurred: {e}")
    return None


@timed
def extract_metadata_filenames(input_csv, output_metadata_file, columns_to_skip):
    """
    Extracts metadata filenames from the input CSV's first row and writes them to the output file.

    Args:
        input_csv (str): Path to the input CSV file.
        output_metadata_file (str): Path to the output metadata file.
        columns_to_skip (set): Set of columns to exclude.

    Returns:
        None: Writes filenames in lowercase to the specified file.
    """
    try:
        with open(input_csv, mode="r", encoding="utf-8-sig") as csv_file:
            reader = csv.reader(csv_file, delimiter=";")
            first_row = next(reader)

        with open(
            output_metadata_file, mode="w", newline="", encoding="utf-8"
        ) as metadata_file:
            writer = csv.writer(metadata_file)
            for filename in first_row:
                if filename.strip().lower() not in columns_to_skip:
                    writer.writerow([filename.strip().lower()])

        print(f"Metadata filenames successfully written to {output_metadata_file}")
    except Exception as e:
        print(f"Error occurred while processing: {e}")
    return None


@timed
def read_metadata_filenames(metadata_filenames_file, encoding="utf-8-sig"):
    """
    Reads metadata filenames from a file and returns them as a list in uppercase.

    Args:
        metadata_filenames_file (str): Path to the metadata filenames file.
        encoding (str): Encoding to use when reading.

    Returns:
        list: List of filenames in uppercase.
    """
    with open(metadata_filenames_file, "r", encoding=encoding) as file:
        filenames = [line.strip().upper() for line in file.readlines() if line.strip()]
    return filenames


@timed
def prepare_directory(directory):
    """
    Prepares a directory by creating it if it doesn't exist.

    Args:
        directory (str): Path to the directory.

    Returns:
        str: Absolute path to the directory.
    """
    os.makedirs(directory, exist_ok=True)
    return os.path.abspath(directory)


@timed
def save_variable_to_csv(df, metadata_filenames, number_of_rows, base_directory):
    """
    Saves each variable in the DataFrame as a separate CSV file.

    Args:
        df (pd.DataFrame): The data as a DataFrame.
        metadata_filenames (list): List of metadata filenames.
        number_of_rows (int): Number of rows in the DataFrame.
        base_directory (str): Base directory for saving CSV files.

    Returns:
        None: Each variable is saved as a CSV in its own subdirectory.
    """
    for name_index, filename in enumerate(metadata_filenames):
        filename = filename.lower()
        metadata_filenames[name_index] = filename

        if filename not in df.columns:
            print(f"Warning: Column '{filename}' not found in DataFrame. Skipping.")
            continue
        if filename not in ["s_sidkrg", "start_time", "stop_time"]:
            filepath = os.path.join(
                base_directory, filename.upper(), f"{filename.upper()}.csv"
            )
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            with open(filepath, "w", encoding="utf-8-sig") as csvfile:
                writer = csv.writer(csvfile, delimiter=";")
                for i in range(number_of_rows):
                    row_data = [
                        df["s_sidkrg"].values[i],
                        df[filename].values[i],
                        "",
                        date.today(),
                        "",
                    ]
                    writer.writerow(row_data)
    return None


@timed
def download_metadata(
    metadata_filenames, url_without_metadata_parameter, base_directory
):
    """
    Downloads metadata files from a URL and saves them in the specified directory.

    Args:
        metadata_filenames (list): List of metadata filenames.
        url_without_metadata_parameter (str): URL to download metadata from.
        base_directory (str): Directory where metadata will be saved.

    Returns:
        None: Saves each metadata file as JSON in its respective directory.
    """
    for filename in metadata_filenames:
        filename = filename.lower()
        if filename not in ["s_sidkrg", "start_time", "stop_time"]:
            metadata_filename = filename.upper() + ".json"
            directory_name = os.path.join(base_directory, filename.upper())
            os.makedirs(directory_name, exist_ok=True)
            url_with_metadata_parameter = url_without_metadata_parameter + filename

            try:
                r = requests.get(url_with_metadata_parameter, timeout=30)
                r.raise_for_status()
                file_path = os.path.join(directory_name, metadata_filename)
                with open(file_path, "wb") as f:
                    f.write(r.content)
                print(f"Successfully retrieved {metadata_filename}")
            except Exception as e:
                print(f"Failed to retrieve metadata: {e}")
    return None


@timed
def validate_downloaded_metadata(metadata_filenames, base_directory):
    """
    Validates the downloaded metadata files against the specified directory.

    Args:
        metadata_filenames (list): A list of metadata filenames to be validated.
        base_directory (str): The directory where the metadata files are located.

    Returns:
        tuple: (valid_metadata, metadata_with_errors)
               valid_metadata is a list of metadata files that passed validation.
               metadata_with_errors is a dictionary with filenames as 
               keys and error messages as values.
    """
    valid_metadata = []
    metadata_with_errors = {}

    for metadata_filename in metadata_filenames:
        validation_errors = validate_metadata(
            metadata_filename.upper(), input_directory=base_directory
        )
        if not validation_errors:
            valid_metadata.append(metadata_filename)
        else:
            metadata_with_errors[metadata_filename] = validation_errors

    return valid_metadata, metadata_with_errors


@timed
def validate_created_dataset(metadata_filenames, base_directory):
    """
    Validates the created dataset files against the specified directory.

    Args:
        metadata_filenames (list): A list of metadata filenames whose
        corresponding datasets need validation.
        base_directory (str): The directory where the dataset files are located.

    Returns:
        tuple: (valid_datasets, datasets_with_errors)
               valid_datasets is a list of datasets that passed validation.
               datasets_with_errors is a dictionary with filenames as keys and error messages as values.
    """
    valid_datasets = []
    datasets_with_errors = {}

    for metadata_filename in metadata_filenames:
        validation_errors = validate_dataset(
            metadata_filename.upper(), input_directory=base_directory
        )
        if not validation_errors:
            valid_datasets.append(metadata_filename)
        else:
            datasets_with_errors[metadata_filename] = validation_errors

    return valid_datasets, datasets_with_errors


@timed
def package_and_encrypt_dataset(
    metadata_filenames, rsa_keys_dir, base_directory, output_directory
):
    """
    Packages and encrypts datasets after they have been validated.

    Args:
        metadata_filenames (list): A list of metadata filenames to be packaged and encrypted.
        rsa_keys_dir (Path): Directory containing the RSA public/private keys.
        base_directory (str): Directory where the datasets are located.
        output_directory (str): Base directory where packaged datasets will be saved.

    Returns:
        tuple: (successful_packages, failed_packages)
               successful_packages is a list of datasets that were successfully packaged.
               failed_packages is a dictionary with filenames as keys and error messages as values.
    """
    successful_packages = []
    failed_packages = {}

    for metadata_filename in metadata_filenames:
        dataset_dir = os.path.join(base_directory, metadata_filename.upper())
        output_dir = os.path.join(output_directory, metadata_filename.upper())

        try:
            os.makedirs(output_dir, exist_ok=True)
            package_dataset(
                rsa_keys_dir=rsa_keys_dir,
                dataset_dir=Path(dataset_dir),
                output_dir=Path(output_dir),
            )
            successful_packages.append(metadata_filename)
        except Exception as e:
            failed_packages[metadata_filename] = str(e)

    return successful_packages, failed_packages


def main():
    """Main function that coordinates validation and displays a final summary."""
    timings = []

    # Load environment variables
    input_csv_filename = os.getenv("INPUT_VARIABLES_CSV_FILE")
    rsa_keys_directory = Path(os.getenv("RSA_KEYS_DIR"))
    base_directory = os.getenv("INPUT_DIR")
    output_directory = os.getenv("OUTPUT_DIR")
    metadata_filenames_file = "Microdata_metadata_variables.csv"
    url_without_metadata_parameter = os.getenv("URL_WITHOUT_METADATA_PARAMETER")
    columns_to_skip = {"s_sidkrg", "start_time", "stop_time"}

    # Run each function and store its result and timing
    _, duration = extract_metadata_filenames(
        input_csv_filename, metadata_filenames_file, columns_to_skip
    )
    timings.append(("Extract Metadata Filenames", duration))

    _, duration = check_csv_symmetry(input_csv_filename)
    timings.append(("Check CSV Symmetry", duration))

    df, duration = read_csv_file(input_csv_filename)
    timings.append(("Read CSV File", duration))

    metadata_filenames, duration = read_metadata_filenames(metadata_filenames_file)
    timings.append(("Read Metadata Filenames", duration))

    input_directory_path, duration = prepare_directory(base_directory)
    timings.append(("Prepare Input Directory", duration))

    output_directory_path, duration = prepare_directory(output_directory)
    timings.append(("Prepare Output Directory", duration))

    _, duration = save_variable_to_csv(
        df, metadata_filenames, df.shape[0], input_directory_path
    )
    timings.append(("Save Variable to CSV", duration))

    _, duration = download_metadata(
        metadata_filenames, url_without_metadata_parameter, input_directory_path
    )
    timings.append(("Download Metadata", duration))

    # Validate downloaded metadata and capture timing
    (valid_metadata, metadata_with_errors), duration = validate_downloaded_metadata(
        metadata_filenames, input_directory_path
    )
    timings.append(("Validate Downloaded Metadata", duration))

    # Validate created datasets and capture timing
    (valid_datasets, datasets_with_errors), duration = validate_created_dataset(
        valid_metadata, input_directory_path
    )
    timings.append(("Validate Created Dataset", duration))

    # Package and encrypt datasets and capture timing
    (successful_packages, failed_packages), duration = package_and_encrypt_dataset(
        valid_datasets, rsa_keys_directory, input_directory_path, output_directory_path
    )
    timings.append(("Package and Encrypt Dataset", duration))

    # Final summary of metadata and dataset validation errors
    print("\nFinal Validation Summary:")

    if metadata_with_errors:
        print("\nMetadata Validation Errors:")
        for filename, errors in metadata_with_errors.items():
            print(colored(f"{filename}:", "red"))
            for error in errors:
                print(f"  - {error}")
    else:
        print("All metadata files passed validation.")

    if datasets_with_errors:
        print("\nDataset Validation Errors:")
        for filename, errors in datasets_with_errors.items():
            print(colored(f"{filename}:", "red"))
            for error in errors:
                print(f"  - {error}")
    else:
        print("All dataset files passed validation.")

    # Summary of packaging results
    print("\nPackage and Encryption Summary:")
    if successful_packages:
        print("Successfully Packaged Datasets:")
        for filename in successful_packages:
            print(f"  - {filename}")
    if failed_packages:
        print("\nFailed to Package Datasets:")
        for filename, error in failed_packages.items():
            print(f"  - {filename}: {error}")

    # Display timing statistics and calculate total time
    table = PrettyTable(["Function", "Time (seconds)"])
    total_time = sum(timing for _, timing in timings)  # Sum of all durations
    for name, timing in timings:
        table.add_row([name, f"{timing:.4f}"])

    print("\nPerformance Timing Statistics:")
    print(table)
    print(f"\nTotal Execution Time: {total_time:.4f} seconds")


if __name__ == "__main__":
    main()
