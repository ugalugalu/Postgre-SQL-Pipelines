# Import the necessary libraries
import pandas as pd
import logging
import matplotlib.pyplot as plt
import psycopg2
import csv
from sqlalchemy import create_engine

def extract_data():
     """
    Returns a dataframe combined from 2 csv files
            
            Parameters:
                    doesnot take in any parameters
            Returns:
                    Returns a Data Frame
    """
    # Load equipment and network sensors
     equipment_sensor = pd.read_csv('equipment_sensor.csv')
     network_sensor = pd.read_csv('network_sensor.csv')
    # Merge the equipment and network sensors file
     merged_sensors = pd.merge(equipment_sensor,network_sensor, how= "left", on = ["ID","date","time"])

    # Use Python logging module to log errors and activities
     logger = logging.getLogger(__name__)
     logger.info("Data extraction completed.")
     print('Extraction successful')

     return merged_sensors

# Transformation function
def transform_data(merged_sensors):
       """
    Returns a cleaned Dataframe.
            
            Parameters:
                    Dataframe
            Returns:
                    Returns a Dataframe
       """
    # Data cleaning and handling missing values
       merged_sensors=merged_sensors.dropna()
       merged_sensors = merged_sensors.drop_duplicates()
       merged_sensors.rename(columns = {'sensor_reading_x':'equipment_readings','sensor_reading_y':'network_readings'},inplace = True)
    
    #Create a new column of average readings and round it to 2 decimal places
       merged_sensors['average_readings'] = round((merged_sensors['equipment_readings']+ merged_sensors['network_readings'])/2,2)

    # Use Python logging module to log errors and activities
       logger = logging.getLogger(__name__)
       logger.info("Data transformation completed.")
       df = merged_sensors
       print('Transformation successful')
       return df

# A glimpse of how analysis can be Done
def data_analysis(df):
      """
    Plots a bar chat of network readings
            
            Parameters:
                    Dataframe(the transformed Database)
            Returns:
                    None
       """
      plt.bar(df['ID'],df['network_readings'])
      plt.xlabel('Sensors')
      plt.ylabel('Sensor Reading')
      plt.title('Sensor Vs Reading')
      plt.show()
      plt.close()

# Loading function
def load_data(df,connection_string):
      """
    Loads a dataframe to a postgre Database Hosted in Google Cloud.
            
            Parameters:
                    Dataframe(the transformed Data Frame),connection string to the database
            Returns:
                    None
       """
       #Connect to the database using alchemy database engine
      engine = create_engine(connection_sting)
      #Load the dataframe to Postgre database
      df.to_sql('network',engine , if_exists = 'append', index = False)
      print("Loading successful")


# A fetching function to confirm that data was loaded to the database
def read_data(connection_string):
      """
    Returns a cleaned Dataframe.
            
            Parameters:
                    connection string to the database
            Returns:
                    Returns a Dataframe
       """
     #Create a connection to the database using pyscop
      conn = psycopg2.connect(connection_sting)
      #Create the query to be executed
      query = "SELECT * FROM network"
      # Execute query,create a dataframe and display the five records.
      queried_frame = pd.read_sql(query,conn)
      print(queried_frame.head())

if __name__ == '__main__':
     connection_sting = 'postgresql://postgres:pipeline@35.239.51.53:5432/postgres'
     merged_sensors = extract_data()
     df = transform_data(merged_sensors)
     data_analysis(df)
     load_data(df,connection_sting)
     read_data(connection_sting)
