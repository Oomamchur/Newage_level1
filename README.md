# Readme

    Level 1

Script to extract image URLs from a given Google Sheet, 
determines the image size, and saves the information to a new Google spreadsheet.

## Installation

Python 3 should be installed.

    https://github.com/Oomamchur/Newage_level1
    cd Newage_level1
    python -m venv venv

On Windows:

    source venv\Scripts\activate

On macOS or Linux:

    source venv/bin/activate

Install requirements:

    pip install -r requirements.txt

## Features
I'm using the gspread library. 
To access the tables, you need to create a service account. Rename your data to "credentials.json".
Here's how to do it with the provided link:

    https://docs.gspread.org/en/v6.0.0/oauth2.html#

Table with the results:

    https://shorturl.at/dsEX0

## Run

    python main.py

If you want to check the script's functionality, you need to enter the email in the .env file. 
The link to the saved Google spreadsheet will be sent to this email.
The execution of the program takes some time.
