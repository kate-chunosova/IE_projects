import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin

class OurAdvancedImputer(BaseEstimator, TransformerMixin):
    """Custom advanced imputation of missing values
        
    Args: 
        name (type): description
        
    Returns: 
        pd.DataFrame: transformed pandas DataFrame with new features
    """
    
    def __init__(self, population_bucket=True):
        self.population_bucket = population_bucket
        self.cluster_population = {}
    
    def fit(self, X, y=None):
        X.loc[X.population.isin([0,1]), 'population'] = np.nan
        for cluster in X.cluster.unique():
            population_mean = X.loc[X.cluster==cluster, 'population'].mean()
            if np.isnan(population_mean):
                self.cluster_population[cluster] = 0
            else:
                self.cluster_population[cluster] = population_mean 
        
        return self
    
    def transform(self, X):
        
        assert isinstance(X, pd.DataFrame)
        
        try:
            X.loc[X.population.isin([0,1]), 'population'] = np.nan
            X.loc[X.population.isnull(),'population'] = X.apply(lambda row: self.cluster_population.get(row.cluster), axis=1)
            
            if self.population_bucket:
                population_log = np.log1p(X.population)
                population_log_binned = pd.cut(population_log, bins=[0,2,6,np.inf], include_lowest=True, labels=[1,2,3])
                X = X.assign(population_binned = population_log_binned)
                                
            return X
            
        except KeyError:
            cols_related = ['population']
            
            cols_error = list(set(cols_related) - set(X.columns))
            raise KeyError('[OurAdvancedImputer] DataFrame does not include the columns:', cols_error)
        
        
        