""" Bacen SGS indexes class data retrieve

The class script below allows you to obtain time series data from the Bacen API.

The indices obtained with this script are: Selic rate, IPCA, IGP-M and INPC.

This script requires that `pandas` and `python-bcb` be installed within the Python
environment you are running this script in.
"""

import asyncio
import datetime
from enum import Enum

import pandas as pd
from bcb import sgs


class Indexes(Enum):
    """
    A class to retrieve Bacen time series indexes

    ...

    Attributes
    ----------
    IPCA : int
        number referring to the time series for ipca
    INPC : int
        number referring to the time series for inpc
    IGP_M : int
        number referring to the time series for igp-m
    SELIC : int
        number referring to the time series for selic
    Start: string
        string referring to start date of series collection

    Methods
    -------
    get_sgs_indexes(ipca, inpc, igp_m, selic, start_date)
        returns a dataframe with the indexes time series requested         
    main()
        create task for get_sgs_indexes method  
    """
  
    IPCA = 433
    INPC = 188
    IGP_M = 189
    SELIC = 4390
    Start = '2010-01-01'
    
    async def get_sgs_indexes(ipca:int, inpc:int, igp_m:int, selic:int, start_date:str) -> pd.DataFrame:
        """ Call Bacen SGS API and returns a dataframe with data retrieved

        Parameters
        ----------
        ipca : int
            number referring to the time series for ipca
        inpc : int
            number referring to the time series for inpc
        igp_m : int
            number referring to the time series for igp-m
        selic : int
            number referring to the time series for selic
        start_date: string
            string referring to start date of series collection
        
        Raises
        ------
        RuntimeError
            Exception raised if API does not return requested data in sufficient time.

        Returns
        -------
        DataFrame
            Data structure returns with date, index and individual value columns.
        """
        today = datetime.date.today()
        first = today.replace(day=1)
        last_month = first - datetime.timedelta(days=1)
 
        try:
            df = sgs.get({
                'IPCA': ipca,               
                'INPC': inpc,
                'IGP-M': igp_m,
                'SELIC': selic}
                ,start=start_date, end=last_month)            
            return df
        except RuntimeError:
            raise 'Time error while executing BACEN api'  
          
          
    async def main():
        """  Main Indexes class function that calls the Bacen API call function       
        
        Returns
        -------
        DataFrame
            Data structure returned after asynchronous task execution.        
        """
        ipca = Indexes.IPCA.value
        inpc = Indexes.INPC.value
        igp_m = Indexes.IGP_M.value
        selic = Indexes.SELIC.value
        start_date = Indexes.Start.value
        task = asyncio.create_task(Indexes.get_sgs_indexes(ipca, inpc, igp_m, selic, start_date))
        await task   
        return task.result()  
