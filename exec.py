import asyncio

from classes.DfToDatabase import DfToDatabase
from classes.SgsIndexes import Indexes

if __name__ == '__main__': 
  df = asyncio.run(Indexes.main())
  engine = DfToDatabase(df = df)

  result = engine.create_engine(engine.df)
  print(result)