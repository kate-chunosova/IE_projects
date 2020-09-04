from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np

class OtherFeatures(BaseEstimator, TransformerMixin):
    """Feature creation based on existing variables
    
    Feature "dry_season" was inspired by the following research: https://lib.ugent.be/fulltxt/RUG01/002/350/680/RUG01-002350680_2017_0001_AC.pdf
    Dry season covers the following months: January(1), February(2), June(6), July(7), August(8), September(9), October(10)
    Wet season covers the following months: March(3), April(4), May(5), November(11), December(12)
    
    Args:
        type_wpt_name (bool): if True creates feature representing type of the waterpoint, default True
        water_per_capita (bool): if True creates feature ratio of amount_tsh to population (amount_tsh / population), default True
        dry_season (bool): if True creates the feature representing if the season is dry (1), 0 otherwise (if it's wet season)
        num_private (bool): if True transforms num_private into True/False feature
        age (bool): if True creates new feature representing the difference between date recorded and construction year
        
    Returns: 
        pd.DataFrame: transformed pandas DataFrame.
    """
    
    def __init__(self, type_wpt_name=True, water_per_capita=True, dry_season=True, num_private=True, age=True):
        self.type_wpt_name = type_wpt_name
        self.water_per_capita = water_per_capita
        self.dry_season = dry_season
        self.num_private = num_private
        self.age = age
        
    def fit(self,X,y=None):    
        return self
    
    def transform(self, X):
        
        assert isinstance(X, pd.DataFrame)
        
        try: 

            if self.type_wpt_name:
                X.loc[:, 'type_wpt_name'] = X.loc[:,'wpt_name'].apply(lambda x: x.split(' ')[0].strip()).replace('Zahanati-Misssion', 'Zahanati')
    
            if self.water_per_capita:
                X.loc[:, 'water_per_capita'] = X.loc[:,'amount_tsh'] / (X.loc[:, 'population'] + 1)
            
            if self.dry_season:
                
                X.loc[:,'date_recorded'] = pd.to_datetime(X.date_recorded)
                X = X.assign(dry_season = X.date_recorded.dt.month)
                X.dry_season = X.dry_season.replace([1,2,3,4,5,6,7,8,9,10,11,12],[1,1,0,0,0,1,1,1,1,1,0,0])
            
            if self.num_private:
                X.loc[:, "num_private"] = X.loc[:, 'num_private'].ne(0).astype(int)
                
            if self.age:
                X.loc[:,'date_recorded'] = pd.to_datetime(X.date_recorded)
                X.loc[:, 'age'] = X.loc[:, 'date_recorded'].dt.year - X.loc[:, 'construction_year'] 
                
            return X


        except KeyError:
            cols_error = list(set(['wpt_name', 'amount_tsh', 'population', 'num_private', 'date_recorded']) - set(X.columns))
            raise KeyError('[OtherFeatures] DataFrame does not include the columns:', cols_error)
            