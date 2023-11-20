""" DataFrama to Postgres table class

The class script below allows you to save dataframe rows to postgres table.

This script requires that `sqlalchemy` and `pandas` be installed within the Python
environment you are running this script in.
"""

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


class DfToDatabase():
    """
      A class to create a postgres database conection and insert Bacen SGS indexes values

      ...

      Attributes
      ----------
      host : string
          postgres database host
      database : string
          postgres database name
      user : string
          user name for access
      password : string
          user password
      port : int
          postgres database port running
      df : dataframe
          dataframe given to insert into postgres database table
      Methods
      -------
      melt_and_prepare_df(df)
          returns a melted dataframe with datekey column      
      create_engine(df)
          create the postgres engine and insert data  
    """

    def __init__(self, df:pd.DataFrame) -> None:
        """__init__

        Args:
            df (pd.DataFrame): Bacen SGS dataframe
        """
        self.env = load_dotenv()         
        self.host = os.getenv('HOST')
        self.database = os.getenv('DATABASE')
        self.user = os.getenv('USER')
        self.password = os.getenv('PASSWORD')
        self.schema = os.getenv('SCHEMA')
        self.df = self.melt_and_prepare_df(df)
    
    
    def __str__(self) -> str:
        """__str__

        Returns:
            str: postgres database connection string
        """
        return f'postgresql://{self.user}:{self.password}@:{self.host}:{self.port}/{self.database}'
    
    
    def melt_and_prepare_df(self, df:pd.DataFrame) -> pd.DataFrame:
        """_summary_

        Args:
            df (pd.DataFrame): Bacen SGS dataframe

        Returns:
            pd.DataFrame: Melted dataframe with datekey column and indexes values in columns
        """
        df['FullDate'] = df.index
        df['DateKey'] = df.FullDate.dt.strftime('%Y%m%d').astype(int)
        df_melted = df.melt(id_vars=('FullDate','DateKey'), var_name='IndexName',value_name='IndexValue')
        return df_melted
    
    
    def create_engine(self, df:pd.DataFrame) -> int | None:
        """ Create the engine to connect to Postgres database and insert the dataframe lines

        Args:
            df (pd.DataFrame): Bacen SGS dataframe

        Returns:
            int | None: Number os rows affected
        """                 
        engine = create_engine(
          f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:5432/{self.database}',
            connect_args={'options': '-csearch_path={}'.format('transactions')}
        )       
        try:
            result = df.to_sql('SgsIndexes', engine, if_exists='append')
            return result
        except ValueError:
            raise "The table already exists and if_exists is 'fail' (the default)."
    
      