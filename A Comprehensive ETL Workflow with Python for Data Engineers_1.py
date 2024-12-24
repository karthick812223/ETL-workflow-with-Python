import requests
import zipfile
import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import json

# Creating a log function to log all the steps and debug
def log_phase(phase):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file_path, 'a') as log:
        log.write(f"{timestamp} - {phase}\n")
# path to extracted files
extract_to_Path = "C:\\Users\\sunda\\unzipped_folder"
# Creating a log path
log_path = "C:\\Users\\sunda\\log"
if not os.path.exists(log_path):
        os.makedirs(log_path)
log_file = "log_file.txt"
log_file_path = os.path.join(log_path,log_file)
log_phase(f"log_file_path is created : {log_file_path}")
# Creating a path after transfermation of the data 
transfer_data_path = "C:\\Users\\sunda\\transfer_data"
if not os.path.exists(transfer_data_path):
        os.makedirs(transfer_data_path)
transfer_data_file = "transformed_data.csv"
transfer_data_file_path = os.path.join(transfer_data_path,transfer_data_file)
log_phase(f"transfer_data_file is created : {transfer_data_file}")
# Creating a individual function for find the type of file
def find_all_csv_files():
        csv_files = glob.glob(os.path.join(extract_to_Path,'*.csv'))
        df_list = []
        for files in csv_files:
                df = pd.read_csv(files)
                df_list.append(df)
        combined_df = pd.concat(df_list,ignore_index=True)
        return combined_df
def find_all_json_files():
        json_files = glob.glob(os.path.join(extract_to_Path,'*.json'))
        df_list =[]
        for files in json_files:
                with open (files,'r') as f:
                        for lines in f:
                                data = json.loads(lines.strip() )
                                df = pd.json_normalize(data)
                                df_list.append(df)
        combined_json = pd.concat(df_list,ignore_index=True)
        return combined_json
def find_all_xml_files():
        xml_files = glob.glob(os.path.join(extract_to_Path,'*.xml'))
        df_list = []
        for file in xml_files:
                tree = ET.parse(file)
                root = tree.getroot()
                data = []
                for elem in root:
                        record = {}
                        for child in elem:
                                record[child.tag] = child.text
                        data.append(record)
                df = pd.DataFrame(data)
                df_list.append(df)
        combined_xml = pd.concat(df_list, ignore_index=True)
        return combined_xml
# Master function for extract all type of file and concatenate
def extract_data():
    df_csv = find_all_csv_files()
    log_phase(f"csv is extracted {extract_to_Path}")
    df_json = find_all_json_files()
    log_phase(f"json is extracted {extract_to_Path}")
    df_xml = find_all_xml_files()
    log_phase(f"xml is extracted {extract_to_Path}")
    # Combine the data into one DataFrame
    combined_df = pd.concat([df_csv, df_json, df_xml], ignore_index=True)
    return combined_df
# Creating a function to transformation data
def transform_data(df):
    if 'height' in df.columns: 
        df['height'] = pd.to_numeric(df['height'], errors='coerce')
        df['height'] = df['height'] * 0.0254
    if 'weight' in df.columns:
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
        df['weight'] = df['weight'] * 0.453592
    log_phase(f"data is transformed successfully")
    return df
# Creating a function to load the data in destination path
def load_data(df):
    df.to_csv(transfer_data_file_path, index=False)
    log_phase(f"Transformed data saved to {transfer_data_file_path}")
    print(f"Transformed data saved to {transfer_data_file_path}")
# Creating a main function a excuetue the ETL process
def main():
    log_phase("Extraction Phase Started")
    extracted_data = extract_data()
    log_phase("Extraction Phase Ended")
    log_phase("Transformation Phase Started")
    transformed_data = transform_data(extracted_data)
    log_phase("Transformation Phase Ended")
    log_phase("Loading Phase Started")
    load_data(transformed_data)
    log_phase("Loading Phase Ended")
if __name__ == "__main__":
    main()
