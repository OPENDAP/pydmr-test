#!/usr/bin/env python3

import requests
import json
import subprocess
import os
import time
import re
from urllib.parse import urlparse
import argparse

CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
CMR_COLLECTIONS_URL = "https://cmr.earthdata.nasa.gov/search/collections.json"
# COLLECTION_DOI is default, can be overridden by command line argument -d
COLLECTION_DOI = "10.5067/MODIS/MCD12Q1.061"
DATA_DIR = os.path.join(os.getcwd(), "hyrax_data")
HYRAX_CONTAINER_NAME = "hyrax"
HYRAX_IMAGE = "opendap/hyrax:1.17.1-126"
HYRAX_PORT = 8080

def run_command(command, check_success=True, capture_output=False, shell=False):
    """
    Executes a shell command.
    :param command: List of strings for the command and its arguments, or a single string if shell=True.
    :param check_success: If True, raises an exception if the command returns a non-zero exit code.
    :param capture_output: If True, captures and returns stdout/stderr.
    :param shell: If True, executes the command through the shell.
    :return: CompletedProcess object if capture_output is True, otherwise None.
    """
    print(f"Executing command: {' '.join(command) if isinstance(command, list) else command}")
    try:
        result = subprocess.run(
            command,
            check=check_success,
            capture_output=capture_output,
            text=True, # Decode stdout/stderr as text
            shell=shell
        )
        if capture_output:
            print("--- Command Output ---")
            print(result.stdout)
            if result.stderr:
                print("--- Command Error Output ---")
                print("--- Start Command Error ---")
                print(result.stderr)
                print("--- End Command Error ---")
            print("----------------------")
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error: Command failed with exit code {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise
    except FileNotFoundError:
        print(f"Error: Command not found. Make sure '{command[0]}' is in your PATH.")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

def get_collection_concept_id(doi):
    """
    Searches CMR for a collection's concept ID using its DOI.
    :param doi: The DOI of the collection.
    :return: The concept ID of the collection, or None if not found.
    """
    params = {
        "doi": doi,
        "page_size": 1,
        "pretty": "true"
    }
    print(f"\nSearching CMR for collection concept ID using DOI: {doi}")
    try:
        response = requests.get(CMR_COLLECTIONS_URL, params=params)
        response.raise_for_status()
        data = response.json()

        entries = data.get("feed", {}).get("entry", [])
        if not entries:
            print(f"No collection found for DOI: {doi}")
            return None

        collection_entry = entries[0]
        concept_id = collection_entry.get("id")
        print(f"Found Collection Concept ID: {concept_id}")
        return concept_id
    except requests.exceptions.RequestException as e:
        print(f"Error searching CMR for collection: {e}")
        return None
    except json.JSONDecodeError:
        print("Error: Could not decode JSON response from CMR for collection.")
        return None


def get_granule_info(collection_concept_id, sort_key):
    """
    Searches Earthdata CMR for granules based on collection concept ID and sort order.
    :param collection_concept_id: The concept ID of the collection to search within.
    :param sort_key: How to sort the results (e.g., "start_date" for first, "-start_date" for last).
    :return: A dictionary containing granule details (title, id, download_url, collection_concept_id).
    """
    params = {
        "collection_concept_id": collection_concept_id,
        "page_size": 1,    # Right now just grab one granule (first or last)
        "sort_key": sort_key,
        "pretty": "true"   # For human-readable JSON response
    }
    print(f"\nSearching CMR for granule with sort key: {sort_key} within collection {collection_concept_id}")
    try:
        # Raise an exception for bad status codes
        response = requests.get(CMR_GRANULES_URL, params=params)
        response.raise_for_status()
        data = response.json()

        entries = data.get("feed", {}).get("entry", [])
        if not entries:
            print(f"No granules found for collection {collection_concept_id} with sort key: {sort_key}")
            return None

        granule_entry = entries[0]
        title = granule_entry.get("title")
        granule_id = granule_entry.get("id") # Granule Concept ID

        download_url = None
        possible_urls = []

        for link in granule_entry.get("links", []):
            href = link.get("href", "")
            rel = link.get("rel", "")

            # Prioritize 'data' or 'producer' links that point to HDF files
            if (rel == "http://esip.opendap.org/ns/esip/data" or \
                rel == "http://esip.opendap.org/ns/esip/producer" or \
                rel == "http://esip.opendap.org/ns/esip/download") and \
               (href.startswith("http://") or href.startswith("https://")) and \
               href.lower().endswith(".hdf"):
                possible_urls.insert(0, href)
            elif (href.startswith("http://") or href.startswith("https://")) and \
                 href.lower().endswith(".hdf"):
                possible_urls.append(href) # Use other direct HDF links as a fallback

        if possible_urls:
            # Prefer HTTPS over HTTP if both are available
            https_urls = [url for url in possible_urls if url.startswith("https://")]
            if https_urls:
                download_url = https_urls[0]
            else:
                download_url = possible_urls[0]

        if not download_url:
            print(f"Warning: No suitable HTTP/HTTPS HDF download URL found for granule: {title}")
            return None

        print(f"Found Granule: {title}")
        print(f"  Granule ID: {granule_id}")
        print(f"  Download URL: {download_url}")

        return {
            "title": title,
            "granule_id": granule_id,
            "download_url": download_url,
            "collection_concept_id": collection_concept_id
        }

    except requests.exceptions.RequestException as e:
        print(f"Error searching CMR: {e}")
        return None
    except json.JSONDecodeError:
        print("Error: Could not decode JSON response from CMR.")
        return None

def download_file(url, destination_path):
    """
    Downloads a file from a given URL to a specified local path.
    :param url: The URL of the file to download.
    :param destination_path: The local path to save the file.
    :return: True if successful, False otherwise.
    """
    print(f"\nDownloading {url} to {destination_path}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download complete.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        # Direct download from Earthdata need an Earthdata login. This script does not currently handle EDL.
        print("NOTE: The 502 Bad Gateway error during download often means Earthdata Login authentication is required.")
        print("This script does not currently implement Earthdata Login for protected data.")
        return False
    except IOError as e:
        print(f"Error saving file to disk: {e}")
        return False

def test_dmrpp(granule_filename):
    """
    Tests the DMR++ sidecar files and data access via the Hyrax server.
    Follows Section 4 of the DMR++ testing instructions.
    :param granule_filename: The base filename of the HDF granule.
    """
    print("\n\t|--------------------------------------------------------------------------------------|"
          f"\n\t|--- Testing DMR++ for {granule_filename} ---|"
          "\n\t|--------------------------------------------------------------------------------------|")
    # OPeNDAP path now includes the full HDF filename before extensions
    base_opendap_path_with_hdf = f"http://localhost:{HYRAX_PORT}/opendap/{granule_filename}"

    test_urls = {
        # Test the direct .dmr endpoint (XML)
        "DMR (XML)": f"{base_opendap_path_with_hdf}.dmr",
        # Test the human-readable .dmr.html endpoint
        "DMR (HTML)": f"{base_opendap_path_with_hdf}.dmr.html",
        # Test the direct .dmrpp endpoint (XML)
        "DMR++ (XML)": f"{base_opendap_path_with_hdf}.dmrpp",
        # Test the human-readable .dmrpp.html endpoint
        "DMR++ (HTML)": f"{base_opendap_path_with_hdf}.dmrpp.html"
    }

    for name, url in test_urls.items():
        print(f"Checking {name} URL: {url}")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            print(f"  {name} check: SUCCESS (Status: {response.status_code})")
            if "HTML" in name:
                print("  Successfully retrieved HTML page.")
        except requests.exceptions.RequestException as e:
            print(f"  {name} check: FAILED - {e}")
        except Exception as e:
            print(f"  An unexpected error occurred during {name} check: {e}")

#---- MAIN SCRIPT ----#
def main():
    # Command line arguments
    parser = argparse.ArgumentParser(description="Python script to test Earthdata granules with Hyrax DMR++.")
    parser.add_argument("-d", "--doi", type=str, default=COLLECTION_DOI,
                        help=f"The DOI for the Earthdata collection (default: {COLLECTION_DOI})")
    parser.add_argument("-s", "--data-dir", type=str, default=DATA_DIR,
                        help=f"The local directory to store downloaded HDF files (default: {DATA_DIR})")
    args = parser.parse_args()

    collection_doi = args.doi
    data_dir = args.data_dir

    print("\n\t/***************************************/"
          "\n\t/*** Starting granule testing script ***/"
          "\n\t/***************************************/")

    # 1. Setup local data directory
    os.makedirs(data_dir, exist_ok=True)
    print(f"Ensured local data directory exists: {data_dir}")

    # Get the collection concept ID first using the DOI
    collection_concept_id = get_collection_concept_id(collection_doi)
    if not collection_concept_id:
        print("Could not retrieve collection concept ID. Exiting.")
        return

    # 2. Docker setup
    print("\n\t|-----------------------------------------|"
          "\n\t|--- Setting up Hyrax Docker Container ---|"
          "\n\t|-----------------------------------------|")
    try:
        # Remove existing container
        run_command(["docker", "rm", "-f", HYRAX_CONTAINER_NAME], check_success=False)

        # Run new Hyrax container
        docker_run_cmd = [
            "docker", "run", "-d",
            "-h", HYRAX_CONTAINER_NAME,
            "-p", f"{HYRAX_PORT}:8080",
            "-v", f"{data_dir}:/usr/share/hyrax", # Mount local data dir to container's /usr/share/hyrax
            "--name", HYRAX_CONTAINER_NAME,
            HYRAX_IMAGE
        ]
        run_command(docker_run_cmd)
        print("Hyrax container should be ready.")
    except Exception as e:
        print(f"Failed to setup Docker container: {e}")
        return

    granules_to_test = []

    # 3. Get first granule info using the collection concept ID
    first_granule = get_granule_info(collection_concept_id, "start_date")
    if first_granule:
        granules_to_test.append(first_granule)

    # 4. Get last granule info using the collection concept ID
    last_granule = get_granule_info(collection_concept_id, "-start_date")
    if last_granule and (not first_granule or first_granule["granule_id"] != last_granule["granule_id"]):
        # Only add last granule if it's different from the first
        granules_to_test.append(last_granule)

    if not granules_to_test:
        print("No granules found to test. Exiting.")
        # Attempt to clean up docker. We can remove this if we want to leave docker running
        print("\n--- Attempting Docker Cleanup ---")
        run_command(["docker", "rm", "-f", HYRAX_CONTAINER_NAME], check_success=False)
        return

    # 5. Loop through granules and test
    for granule_info in granules_to_test:
        download_url = granule_info["download_url"]
        parsed_url = urlparse(download_url)
        granule_filename = os.path.basename(parsed_url.path)
        local_hdf_path = os.path.join(data_dir, granule_filename)

        if not download_file(download_url, local_hdf_path):
            print(f"Skipping processing for {granule_filename} due to download failure.")
            continue

        print("\n\t|--------------------------------------------------------------------------------------|"
              f"\n\t|--- Running gen_dmrpp_side_car for {granule_filename} ---|"
              "\n\t|--------------------------------------------------------------------------------------|")
        # Ensure the filename passed to docker exec is relative to /usr/share/hyrax
        # which is the mounted directory.
        dmrpp_command = [
            "docker", "exec", "-it",
            "-w", "/usr/share/hyrax", # Working directory inside the container
            HYRAX_CONTAINER_NAME,
            "gen_dmrpp_side_car",
            "-i", granule_filename, # Filename relative to /usr/share/hyrax
            "-H", "-U"
        ]
        try:
            run_command(dmrpp_command)
            print(f"Successfully ran gen_dmrpp_side_car for {granule_filename}.")
            test_dmrpp(granule_filename)
        except Exception as e:
            print(f"Failed to generate DMR++ sidecar or test for {granule_filename}: {e}")

    # 6. Clean up
    print("\n\n--- Cleaning up Docker Container ---")
    run_command(["docker", "rm", "-f", HYRAX_CONTAINER_NAME], check_success=False)
    # Optionally, you can remove the downloaded data:
    # import shutil
    # shutil.rmtree(data_dir)
    # print(f"Removed local data directory: {data_dir}")

    print("\n\t|--------------------------|"
          "\n\t|Granule testing, complete.|"
          "\n\t|--------------------------|\n")

if __name__ == "__main__":
    main()

