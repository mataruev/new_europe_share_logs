import threading

import streamlit as st
import altair as alt
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
            local_dir = "downloaded_files"
            with ftputil.FTPHost(ftp_host, ftp_user, ftp_pass) as host:
                remote_dir = "/your/remote/directory"

                # host.chdir(remote_dir)
                list_dir = host.listdir(host.curdir)
                downloaded_files_list = [f for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))]

                is_file_downloaded = False
                for name in list_dir:
                    if name.endswith(".jtz") and (not (name in downloaded_files_list)):
                        if host.download(name, os.path.join(local_dir, name)):
                            st.write(f"Downloaded {name}")
                            is_file_downloaded = True

        except Exception as e:
            st.error(f"Error: {str(e)}")
        finally:
            if is_file_downloaded:
                st.rerun()
        time.sleep(30)


def calculate_twa(row):
    if row['twa'] < 180:
        return row['twa']
    else:
        return row['twa'] - 360


def draw_chart(df_or, var_list):
    df = df_or.copy()
    df['utc_datetime'] = df['utc_datetime'].dt.tz_localize('UTC')

    chart = alt.Chart(df.melt(id_vars='utc_datetime', var_name='variable', value_name='value')).mark_line().encode(
        x=alt.X('utc_datetime:T', axis=alt.Axis(format='%Y-%m-%d %H:%M:%S', title='Your local Time')),
        y=alt.Y('value:Q', title='Value'),
        color='variable:N',
        tooltip=['utc_datetime:T', 'value:Q', 'variable:N']
    ).transform_filter(
        alt.FieldOneOfPredicate(field='variable', oneOf=var_list)
    ).properties(
        # title=f'Chart for {" ".join(var_list)}',
        width=600,
        height=400
    )

    # Manually set y-axis domain based on the minimum and maximum values of both variables
    y_min = df[var_list].min().min()
    y_max = df[var_list].max().max()
    chart = chart.encode(alt.Y(f'value:Q', scale=alt.Scale(domain=(y_min, y_max))))
    # Move the legend to the bottom
    chart = chart.configure_legend(orient='bottom')
    # Display the chart in Streamlit
    st.altair_chart(chart, use_container_width=True)


def pars_draw(file_name):
    track = AdrenaTrack(file_name)
    df = track.trz_parsing(tasks=0, show_progress=False)
    df['twa_c'] = df.apply(calculate_twa, axis=1)
    draw_chart(df, ["heading_true", "cog"])
    draw_chart(df, ["bsp", "sog"])
    draw_chart(df, ["tws"])
    draw_chart(df, ["twa_c"])
    draw_chart(df, ["twd"])
    draw_chart(df, ["cur_speed"])
    draw_chart(df, ["cur_dir"])

    # st.line_chart(df, x='utc_datetime', y='tws')
    # st.line_chart(df, x='utc_datetime', y='twd')
    # st.line_chart(df, x='utc_datetime', y='twa_c')
    # st.line_chart(df, x='utc_datetime', y='bsp')
    # st.line_chart(df, x='utc_datetime', y='heading_true')


def main():
    st.title("IMOCA New Europe live DATA")

    option = st.selectbox("Select a page:", ["Recent data Graphs", "All Adrena Files"])
    local_dir = "downloaded_files"

    if option == "Recent data Graphs":
        st.header("Graphs Page")

        # Create a directory for downloaded files
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        st.write("Data will updates every 30 minutes...")

        # Process and display the downloaded data
        files = os.listdir(local_dir)
        files.sort(reverse=True)
        file_path = os.path.join(local_dir, files[0])
        if file_path.endswith("jtz"):
            try:
                st.write(f"Processing {file_path}...")
                pars_draw(file_path)

            except Exception:
                try:
                    file_path = os.path.join(local_dir, files[1])
                    st.write(f"Processing {file_path}...")
                    pars_draw(file_path)
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
