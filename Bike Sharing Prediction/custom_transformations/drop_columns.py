from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np
    
class DropColumns(BaseEstimator, TransformerMixin):
    """Drops origincal columns if selected. 
    
    Args:
        season (bool): if True drops this column, default False
        yr (bool): if True drops this column, default False
        hr (bool): if True drops this column, default False
        holiday (bool): if True drops this column, default False
        weekday (bool): if True drops this column, default False
        workingday (bool): if True drops this column, default False
        weathersit (bool): if True drops this column, default False
        temp (bool): if True drops this column, default False
        atemp (bool): if True drops this column, default False
        hum (bool): if True drops this column, default False
        windspeed (bool): if True drops this column, default False
        mnth (bool): if True drops this column, default False
    
    Returns:
        pd.DataFrame: transformed pandas DataFrame.
    """
    
    def __init__(self, season=False, yr=False, hr=False, holiday=False,
                 weekday=False, workingday=False, weathersit=False, temp=False, 
                 atemp=False, hum=False, windspeed=False, mnth=False):
        self.season = season
        self.yr = yr
        self.hr = hr
        self.holiday = holiday 
        self.weekday = weekday
        self.workingday = workingday
        self.weathersit = weathersit
        self.temp = temp
        self.atemp = atemp
        self.hum = hum
        self.windspeed = windspeed
        self.mnth = mnth
        
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        
        assert isinstance(X, pd.DataFrame)
        
        try:
        
            if self.season:
                X = X.drop('season', axis=1)

            if self.yr:
                X = X.drop('yr', axis=1)

            if self.hr:
                X = X.drop('hr', axis=1)

            if self.holiday:
                X = X.drop('holiday', axis=1)

            if self.weekday:
                X = X.drop('weekday', axis=1)

            if self.workingday:
                X = X.drop('workingday', axis=1)

            if self.weathersit:
                X = X.drop('weathersit', axis=1)

            if self.temp:
                X = X.drop('temp', axis=1)

            if self.atemp:
                X = X.drop('atemp', axis=1)

            if self.hum:
                X = X.drop('hum', axis=1)

            if self.windspeed:
                X = X.drop('windspeed', axis=1)
                
            if self.mnth:
                X = X.drop('mnth', axis=1)
        
        except KeyError:
            cols_error = list(set(['season', 'yr', 'mnth', 'hr', 'holiday', 'weekday', 'workingday', 
                                   'weathersit', 'temp', 'atemp', 'hum', 'windspeed']) - set(X.columns))
            raise KeyError('[DropColumns] DataFrame does not include the columns:', cols_error)
        
        return X
            