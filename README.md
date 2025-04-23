# Otodom_Data_Analysis


🏠 Otodom Analysis Project

This project analyzes real estate data from the Otodom website using Snowflake, Python, Google Sheets, and Brightdata.

📁 Project Overview

This repository contains:

Snowflake SQL scripts for data import and transformation.
Python scripts (provided in separate files).
Instructions for setting up Google Sheets integration for translation.
Final analysis problems with separate solution documentation.
🚀 Project Workflow

🔹 STEP A: Explore Otodom Website
Visit Otodom to understand the real estate listings and data structure.

🔹 STEP B: Download Dataset from Brightdata
Go to Brightdata and download the Otodom dataset.
Export the dataset to a Snowflake stage as described in techTFQ’s YouTube tutorial.
🔹 STEP C: Set Up Snowflake
1. Create Snowflake Account and Install SnowSQL

Create Snowflake account
Install SnowSQL
2. Connect and Create Database & Warehouse

STEP D: Transform and Translate Data
1. Geolocation Transformation

Use Python script fetch_address_Otodom_Analysis.py to convert coordinates into address details. Creates:

OTODOM_DATA_FLATTEN_ADDRESS_FULL
Alternatively, load from CSV if not using Python.

2. Translate Titles to English

Use translate_text_gsheet_Otodom_Analysis.py to split and upload data to Google Sheets.
Wait 30-60 mins for translation.
Run load_data_gsheet_to_SF_Otodom_Analysis.py to load translated data back to Snowflake as:
OTODOM_DATA_FLATTEN_TRANSLATE
🔹 STEP E: Final Transformed Table
Create final enriched table combining address, translation, and computed metrics:


STEP F: Solve Business Questions
Using OTODOM_DATA_TRANSFORMED, answer key real-estate questions:

Average rent by room count.
Suburbs with 90-100 m² apartments priced 800K–1M.
Expected apartment size for 3K–4K PLN rent.
Most expensive apartments.
Percentage of private vs business ads.
Avg. sale price for 50–70 m² apartments.
Rental prices in Warsaw by suburb and surface area.
Top 3 luxurious neighborhoods in Warsaw.
Affordable suburbs for 40–60 m² apartments.
Suburb with most and least private ads.
Avg. rental and sale prices by major city.
