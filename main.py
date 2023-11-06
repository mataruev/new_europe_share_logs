import threading

import streamlit as st
import ftputil
import os
import time

from adrena import AdrenaTrack


ftp_host = st.secrets['data']['FTP_HOST']
ftp_user = st.secrets['data']['FTP_USER']
ftp_pass = st.secrets['data']['FTP_PASS']


# Function to connect to the FTP server and download files
def download_files():
    while True:
        try:
            with ftputil.FTPHost(ftp_host, ftp_user, ftp_pass) as host:
                remote_dir = "/your/remote/directory"
                local_dir = "downloaded_files"

                # host.chdir(remote_dir)
                list_dir = host.listdir(host.curdir)
                is_file_downloaded = False
                for name in list_dir:
                    if name.endswith(".jtz"):
                        if host.download(name, os.path.join(local_dir, name)):
                            st.write(f"Downloaded {name}")
                            is_file_downloaded = True

        except Exception as e:
            st.error(f"Error: {str(e)}")
        finally:
            if is_file_downloaded:
                st.rerun()
        time.sleep(600)


def main():
    st.title("IMOCA New Europe recent DATA")

    option = st.selectbox("Select a page:", ["Recent Graphs", "All Adrena Files"])
    local_dir = "downloaded_files"

    if option == "Recent Graphs":
        st.header("Graphs Page")

        # Create a directory for downloaded files
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        st.write("Data will updates every 30 minutes...")

        # Process and display the downloaded data
        files = os.listdir(local_dir)
        files.sort(reverse=True)
        file_path = os.path.join(local_dir, files[0])

        if file_path.endswith(".jtz"):

            st.write(f"Processing {file_path}...")
            try:
                track = AdrenaTrack(file_path)
                df = track.trz_parsing(tasks=0, show_progress=False)

                st.line_chart(df, x='utc_datetime', y='tws')
                st.line_chart(df, x='utc_datetime', y='twa')
                st.line_chart(df, x='utc_datetime', y='bsp')
                st.line_chart(df, x='utc_datetime', y='heading_true')
            except Exception:
                st.write("Latest data is not correct, please wait for next update")

    elif option == "All Adrena Files":
        st.header("Download Links Page")

        # Display download links for the files
        st.write("Download the following files:")
        files = os.listdir(local_dir)
        files.sort(reverse=True)
        for file in files:
            # download_link = f'<a href="{local_dir}/{file}" download="{file}">Download {file}</a>'
            # st.markdown(download_link, unsafe_allow_html=True)
            file_path = os.path.join(local_dir, file)
            with open(file_path, 'rb') as f:
               st.download_button(file, f, file_name=file_path)


if __name__ == '__main__':
    # Create a separate thread for downloading files
    download_thread = threading.Thread(target=download_files)
    download_thread.daemon = True
    download_thread.start()

    # Run the Streamlit app
    main()
