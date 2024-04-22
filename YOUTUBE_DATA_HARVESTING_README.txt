Project Title: DATA-HARVESTING-AND-WAREHOUSING
LinkedIn: https://www.linkedin.com/in/barath--kannan

REQUIREMENTS:
The task involves developing a Streamlit application enabling users to access and analyze data from multiple YouTube channels. The application allow users to input a YouTube channel ID to retrieve relevant data such as channel name, subscribers, total video count, playlist ID, video ID, likes, and comments for each video using the Google API. It should support collecting data for up to 10 different YouTube channels and storing them in a data lake with the option to choose between MySQL or PostgreSQL for storage. Additionally, the application should facilitate searching and retrieving data from the SQL database, offering various search options, including joining tables to acquire comprehensive channel details.

TOOLS USED:
STREAMLIT: Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.

PYTHON: Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

MYSQL: MySQL is used to store data in tables that map to objects. Each table has a schema defining what columns each row of the table will have. Developers can reliably store and retrieve many data types, including text, numbers, dates, times, and even JSON.

LIBRARIES USED:
1) requests
2) json
3) mysql.connector
4) streamlit
5) pandas 

RESULT:
	This code creates a Streamlit application for harvesting and analyzing data from YouTube channels, utilizing the Google API and SQL databases. It allows users to input a YouTube channel ID, retrieve data such as channel details, playlists, videos, and comments, and store them in a MySQL. The application consists of functions for connecting to the database, creating necessary tables, fetching data from the YouTube API, inserting data into the database, and querying the database to retrieve analytical insights. It also includes a user interface with options to search and display various analytics based on predefined questions, such as the most viewed videos, channels with the most videos, total views per channel, average video duration per channel, etc. The application is designed to provide users with comprehensive insights into YouTube channel data in a streamlined and interactive manner.
