import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin

class OurSimpleImputer(BaseEstimator, TransformerMixin):
    """Custom imputation of missing values
        
    Args: 
        name (type): description
        
    Returns: 
        pd.DataFrame: transformed pandas DataFrame with new features
    """
    
    def __init__(self, categorical=True,coords=True,permit=True, construction_year=True):
        self.permit = permit
        self.categorical = categorical
        self.coords = coords
        self.lga_coords = {}
        self.construction_year = construction_year
        self.extraction_dict = {}
    
    def fit(self, X, y=None):
        # saving center coordinates of each LGA
        if self.coords:
            X.loc[X.longitude == 0, ['longitude','latitude']] = np.nan
            for lga in X.lga.unique():
                if lga == 'Geita':                    
                    lat = -2.869440
                    lon = 32.234906
                else:
                    lat = X.loc[X.lga == lga, 'latitude'].mean()
                    lon = X.loc[X.lga == lga, 'longitude'].mean()
                self.lga_coords[lga] = (lat, lon)
        
        if self.construction_year:
            X.loc[X.construction_year.isin([0]), 'construction_year'] = np.nan
            for cluster in X.extraction_type.unique():
                year_mean = X.loc[X.extraction_type==cluster, 'construction_year'].median()
                if np.isnan(year_mean):
                    self.extraction_dict[cluster] = 0
                else:
                    self.extraction_dict[cluster] = year_mean
        
        return self
    
    def transform(self, X):
        
        assert isinstance(X, pd.DataFrame)
        
        try:
            
            # categorical features
            if self.categorical:
                na_names = ['Not known', 'not known', '-', 'No', 'no', 'Unknown', '0', 'none']
                X = X.replace(na_names, np.nan)
                X = X.fillna({
                    'funder': 'unknown', 
                    'installer': 'unknown', 
                    'management': 'unknown', 
                    'payment': 'unknown', 
                    'water_quality': 'unknown', 
                    'quantity': 'unknown', 
                    'source': 'unknown',
                    'wpt_name': 'unknown',
                    'scheme_management': 'unknown',
                    'permit': self.permit
                })
                X.loc[:, 'permit'] = X.loc[:, 'permit'].astype(int)
            
            # coordinates
            if self.coords:
                X.loc[X.longitude==0, ['longitude', 'latitude']] = np.nan
                X.loc[X.latitude.isnull(),'latitude'] = X.apply(lambda row: self.lga_coords.get(row.lga)[0], axis=1)
                X.loc[X.longitude.isnull(),'longitude'] = X.apply(lambda row: self.lga_coords.get(row.lga)[1], axis=1)
                
            # construction year
            if self.construction_year:
                X.loc[X.construction_year==0, 'construction_year'] = np.nan
                X.loc[X.construction_year.isnull(),'construction_year'] = X.apply(lambda row: self.extraction_dict.get(row.extraction_type), axis=1)
                    
            return X
            
        except KeyError:
            cols_related = ['funder', 'installer', 'management', 'payment',
                            'water_quality', 'quantity', 'source', 'scheme_management',
                           'latitude','longitude','lga', 'construction_year']
            
            cols_error = list(set(cols_related) - set(X.columns))
            raise KeyError('[OurImputer] DataFrame does not include the columns:', cols_error)
        
        
        