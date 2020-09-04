import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin

class DataCorrection(BaseEstimator, TransformerMixin):
    """ Text
        
    Args: 
        name (type): description
        
    Returns: 
        pd.DataFrame: transformed pandas DataFrame with new features
    """
    
    def __init__(self, installer=False, funder=False):
        self.installer = installer
        self.funder = funder
    
    def fit(self, X, y=None):    
        return self
    
    def transform(self, X):
        
        assert isinstance(X, pd.DataFrame)
        
        try:
        
            if self.installer:
                gov_installer = X.installer.str.lower().str.slice(stop=5).isin(['gover', 'centr','tanza', 'cetra'])
                X.loc[gov_installer, 'installer'] = 'government'
                X.loc[:, 'installer'] = X.apply(lambda row: row.installer.lower()[:4], axis=1) 
            
            if self.funder:
                funder_installer = X.funder.str.lower().str.slice(stop=5).isin(['gover', 'centr','tanza', 'cetra'])
                X.loc[funder_installer, 'funder'] = 'government'
                X.loc[:, 'funder'] = X.apply(lambda row: row.funder.lower()[:4], axis=1) 
                
            return X
            
        except KeyError:
            cols_related = ['installer', 'funder']
            
            cols_error = list(set(cols_related) - set(X.columns))
            raise KeyError('[DataCorrection] DataFrame does not include the columns:', cols_error)
        
        
        