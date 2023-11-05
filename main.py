import streamlit as st
import pandas as pd
import ftputil
import os
import time
import zipfile
import toml


# Function to read FTP credentials from the config file
def read_ftp_credentials():
    try:
        config = toml.load("config.toml")
        return config['data']
    except FileNotFoundError:
        st.error("Config file not found.")
        st.stop()


ftp_credentials = read_ftp_credentials()
ftp_host = ftp_credentials['FTP_HOST']
ftp_user = ftp_credentials['FTP_USER']
ftp_pass = ftp_credentials['FTP_PASS']


# Function to connect to the FTP server and download files
def download_files():
    try:
        with ftputil.FTPHost(ftp_host, ftp_user, ftp_pass) as host:
            remote_dir = "/your/remote/directory"
            local_dir = "downloaded_files"

            # host.chdir(remote_dir)
            list_dir = host.listdir(host.curdir)
            for name in list_dir:
                if name.endswith(".jtz"):
                    if host.download(name, os.path.join(local_dir, name)):
                        st.write(f"Downloaded {name}")

    except Exception as e:
        st.error(f"Error: {str(e)}")


# Function to unzip a file
def unzip_file(file_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall("unzipped_files")


# Function to process data and create graphs
def process_data(data):
    df = pd.DataFrame(data, columns=["Time", "Value"])
    df["Time"] = pd.to_datetime(df["Time"])
    return df


def main():
    st.title("NEW EUROPE RECENT DATA")

    option = st.selectbox("Select a page:", ["Graphs", "Download Links"])

    if option == "Graphs":
        st.header("Graphs Page")

        # Create a directory for downloaded files
        if not os.path.exists("downloaded_files"):
            os.makedirs("downloaded_files")

        # Create a directory for unzipped files
        if not os.path.exists("unzipped_files"):
            os.makedirs("unzipped_files")

        st.write("Checking FTP every 10 minutes...")

        while True:
            download_files()
            time.sleep(600)  # Sleep for 10 minutes

            # Process and display the downloaded data
            files = os.listdir("downloaded_files")
            for file in files:
                file_path = os.path.join("downloaded_files", file)

                if file.endswith(".jtz"):
                    st.write(f"Unzipping {file}...")
                    unzip_file(file_path)
                    st.write(f"Processing {file}...")

                    # Replace this with your data processing logic
                    data = [("2023-01-01 00:00:00", 10), ("2023-01-01 00:10:00", 20)]

                    df = process_data(data)

                    st.write("Data in DataFrame:")
                    st.write(df)

                    st.line_chart(df.set_index("Time"))

    elif option == "Download Links":
        st.header("Download Links Page")

        # Display download links for the files
        st.write("Download the following files:")
        files = os.listdir("downloaded_files")
        for file in files:
            st.markdown(f"**Download {file}**")
            st.write(f"[Download {file}](downloaded_files/{file})")


if __name__ == '__main__':
    main()
