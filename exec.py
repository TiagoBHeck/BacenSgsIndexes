import asyncio

import pandas as pd

from classes.DfToDatabase import DfToDatabase
from classes.SgsIndexes import Indexes


class Exec():  
  
    def __init__(self) -> pd.DataFrame:
        self.df = asyncio.run(Indexes.main())
      
      
    def save_to_db(self, df:pd.DataFrame) -> int | None:
        engine = DfToDatabase(self.df)
        result = engine.create_engine(engine.df)
        return result
  

if __name__ == '__main__': 
    exec = Exec()
    try:
      rows = exec.save_to_db(exec.df)
      print(f'Total number of rows inserted in database: {rows}')
    except ValueError:
      raise "ValueError raised on traying to save rows"
    