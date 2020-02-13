from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np

class AddNewFeatures(BaseEstimator, TransformerMixin):
    """Feature creation based on existing weather variables
    
    Args:
        atemp_vs_hum (bool): if True creates feature ratio of atemp to hum (temp / hum), default True
        windspeed_vs_atemp (bool): if True creates feature ratio of windspeed to atemp (windspeed / atemp), default True
        windspeed_atemp (bool): if True creates feature multiplication of windspeed and atemp (windspeed * atemp), default True
        windspeed_hum (bool): if True creates feature multiplication of windspeed and hum (windspeed * hum), default True
    
    Returns: 
        pd.DataFrame: transformed pandas DataFrame.
    """
    
    def __init__(self, atemp_vs_hum=True, windspeed_vs_atemp=True,
                 windspeed_atemp=True, windspeed_hum=True):
        self.atemp_vs_hum = atemp_vs_hum
        self.windspeed_vs_atemp=windspeed_vs_atemp
        self.windspeed_atemp = windspeed_atemp
        self.windspeed_hum = windspeed_hum
    
    def fit(self,X,y=None):    
        return self
    
    def transform(self, X):
        
        assert isinstance(X, pd.DataFrame)
        
        try: 

            if self.atemp_vs_hum:
                X.loc[:, 'atemp_vs_hum'] = X.loc[:,'atemp'] / (X.loc[:, 'hum']+1)

            if self.windspeed_vs_atemp:
                X.loc[:, 'windspeed_vs_atemp'] = X.loc[:, 'windspeed'] / (X.loc[:,'atemp']+1)

            if self.windspeed_atemp:
                X.loc[:, 'windspeed_atemp'] = X.loc[:, 'windspeed'] * X.loc[:,'atemp']

            if self.windspeed_hum:
                X.loc[:, 'windspeed_hum'] = X.loc[:,'windspeed'] * X.loc[:,'hum']
                
        except KeyError:
            cols_error = list(set(['temp', 'atemp', 'hum', 'windspeed']) - set(X.columns))
            raise KeyError('[AddNewFeatures] DataFrame does not include the columns:', cols_error)
            
        return X