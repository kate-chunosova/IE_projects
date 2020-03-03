from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np


class Interactions(BaseEstimator, TransformerMixin):
    """Feature creation based on existing categorical variables, interactions between them
    
    Args:
        scheme_management_payment (bool): if True creates feature interaction of scheme_management and payment (scheme_management + payment), default True
        basin_source (bool): if True creates feature interaction of basin and source (basin + source), default True
        source_waterpoint_type (bool): if True creates feature interaction of source and waterpoint_type (source + waterpoint_type), default True
        extraction_waterpoint_type (bool): if True creates feature interaction of extraction and waterpoint_type (extraction + waterpoint_type), default 
        True
        water_quality_quantity (bool): if True creates feature interaction of water_quality and quantity (water_quality + quantity), default True
        source_extraction_type (bool): if True creates feature interaction of source and extraction_type (source + extraction_type), default True
        extraction_type_payment (bool): if True creates feature interaction of payment and extraction_type (payment + extraction_type), default True
           
    Returns: 
        pd.DataFrame: transformed pandas DataFrame.
    """
    
    def __init__(self, scheme_management_payment=True, basin_source=True, 
                 source_waterpoint_type=True, extraction_waterpoint_type=True, 
                 water_quality_quantity=True, source_extraction_type=True, extraction_type_payment=True):
        self.scheme_management_payment = scheme_management_payment
        self.basin_source = basin_source
        self.source_waterpoint_type = source_waterpoint_type
        self.extraction_waterpoint_type = extraction_waterpoint_type
        self.water_quality_quantity = water_quality_quantity
        self.source_extraction_type = source_extraction_type
        self.extraction_type_payment = extraction_type_payment
 
    
    def fit(self,X,y=None):    
        return self
    
    def transform(self, X):
        
        assert isinstance(X, pd.DataFrame)
        
        try: 

            if self.scheme_management_payment:
                X.loc[:, 'scheme_management_payment'] = X.loc[:,'scheme_management'] + '_' + X.loc[:,'payment']
                                                           
            if self.basin_source:
                X.loc[:, 'basin_source'] = X.loc[:,'basin'] + '_' + X.loc[:,'source']
            
            if self.source_waterpoint_type:
                X.loc[:, 'source_waterpoint_type'] = X.loc[:,'source'] + '_' + X.loc[:,'waterpoint_type']
            
            if self.extraction_waterpoint_type:
                X.loc[:, 'extraction_waterpoint_type'] = X.loc[:,'extraction_type'] + '_' + X.loc[:,'waterpoint_type']    
            
            if self.source_extraction_type:
                X.loc[:, 'source_waterpoint_type'] = X.loc[:,'source'] + '_' + X.loc[:,'extraction_type']
            
            if self.water_quality_quantity:
                X.loc[:, 'water_quality_quantity'] = X.loc[:,'water_quality'] + '_' + X.loc[:,'quantity']
            
            if self.extraction_type_payment:
                X.loc[:, 'extraction_type_payment'] = X.loc[:,'extraction_type'] + '_' + X.loc[:,'payment']
                
            return X                                                                                                                     
                                                                      
        except KeyError:
            cols_error = list(set(['scheme_management', 'basin', 'source', 'population', 'payment', 'waterpoint_type', 'extraction_type', 'water_quality', 'quantity' ]) - set(X.columns))
            raise KeyError('[Interactions] DataFrame does not include the columns:', cols_error)
            
    