from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np
    
class Dummification(BaseEstimator, TransformerMixin):
    """Custom dummification of categorical features Month, Weathersit and Season if present. Other categorical features are OneHot encoded in usual manner. 
    
    Returns: 
        pd.DataFrame: transformed pandas DataFrame.
    """
    
    def __init__(self):
        pass
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        
        assert isinstance(X, pd.DataFrame)
        
        # custom dummification
        
        # month dummification
        if 'mnth' in X.columns:
            X.loc[:, 'mnth_2'] = (X.loc[:, 'mnth'] == 2).astype(int)
            X.loc[:, 'mnth_3'] = (X.loc[:, 'mnth'] == 3).astype(int)
            X.loc[:, 'mnth_4'] = (X.loc[:, 'mnth'] == 4).astype(int)
            X.loc[:, 'mnth_5'] = (X.loc[:, 'mnth'] == 5).astype(int)
            X.loc[:, 'mnth_6'] = (X.loc[:, 'mnth'] == 6).astype(int)
            X.loc[:, 'mnth_7'] = (X.loc[:, 'mnth'] == 7).astype(int)
            X.loc[:, 'mnth_8'] = (X.loc[:, 'mnth'] == 8).astype(int)
            X.loc[:, 'mnth_9'] = (X.loc[:, 'mnth'] == 9).astype(int)
            X.loc[:, 'mnth_10'] = (X.loc[:, 'mnth'] == 10).astype(int)
            X.loc[:, 'mnth_11'] = (X.loc[:, 'mnth'] == 11).astype(int)
            X.loc[:, 'mnth_12'] = (X.loc[:, 'mnth'] == 12).astype(int)
            X = X.drop('mnth', axis=1)
        
        # weathersit dummification
        if 'weathersit' in X.columns:
            X.loc[:, 'weathersit_2'] = (X.loc[:, 'weathersit'] == 2).astype(int)
            X.loc[:, 'weathersit_3'] = (X.loc[:, 'weathersit'] == 3).astype(int)
            X.loc[:, 'weathersit_4'] = (X.loc[:, 'weathersit'] == 4).astype(int)
            X = X.drop('weathersit', axis=1)
        
        # season dummification if present
        if 'season' in X.columns:
            X.loc[:, 'season_2'] = (X.loc[:, 'season'] == 2).astype(int)
            X.loc[:, 'season_3'] = (X.loc[:, 'season'] == 3).astype(int)
            X.loc[:, 'season_4'] = (X.loc[:, 'season'] == 4).astype(int)
            X = X.drop('season', axis=1)
        
        return pd.get_dummies(X, drop_first=True)
            