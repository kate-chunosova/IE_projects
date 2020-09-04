from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np

class Distance(BaseEstimator, TransformerMixin):
    """Feature creation based on existing geographic variables
    
    Args:
        distance_to_Dodoma (bool): if True creates manhattan distance from the waterpoint to Dodoma, default True
        distance_to_Salaam (bool): if True creates manhattan distance from the waterpoint to Salaam, default True
        strategy (str): 'manhattan' or 'eucledian' distance
    
    Returns: 
        pd.DataFrame: transformed pandas DataFrame.
    """
    
    def __init__(self, distance_to_Dodoma=True, distance_to_Salaam=True, strategy='manhattan'):
        self.distance_to_Dodoma = distance_to_Dodoma
        self.distance_to_Salaam = distance_to_Salaam
        self.strategy = strategy
        
    def fit(self,X,y=None):    
        return self
    
    def transform(self, X):
        
        assert isinstance(X, pd.DataFrame)
        
        try: 
            dodoma = (-6.1630, 35.7516)
            salaam = (-6.7924, 39.2083)

            if self.strategy == 'manhattan':
                if self.distance_to_Dodoma:
                    X.loc[:, 'distance_to_Dodoma'] = np.abs(X.loc[:,'longitude'] - dodoma[1]) + np.abs(X.loc[:, 'latitude'] - dodoma[0])

                if self.distance_to_Salaam:
                    X.loc[:, 'distance_to_Salaam'] = np.abs(X.loc[:,'longitude'] - salaam[1]) + np.abs(X.loc[:, 'latitude'] - salaam[0])
            
            elif self.strategy == 'eucledian':
                if self.distance_to_Dodoma:
                    X.loc[:, 'distance_to_Dodoma'] = np.sqrt((X.loc[:,'longitude'] - dodoma[1])**2 + (X.loc[:, 'latitude'] - dodoma[0])**2)
                
                if self.distance_to_Salaam:
                    X.loc[:, 'distance_to_Salaam'] = np.sqrt((X.loc[:,'longitude'] - salaam[1])**2 + (X.loc[:, 'latitude'] - salaam[0])**2)
                    
            else:
                raise KeyError('Strategy is wrong. Should be either "manhattan" or "eucledian"')
            
            return X

        except KeyError:
            cols_error = list(set(['longtitude', 'latitude']) - set(X.columns))
            raise KeyError('[Distance] DataFrame does not include the columns:', cols_error)
            
      