import numpy as np
import pandas as pd
from typing import List

class DoubleValidationEncoderNumerical:
    """
    Encoder with validation within

    """

    def __init__(self, cols: List, encoder, folds):

        """
        Parameters
        ----------

        :param cols: Categorical columns
        :param encoder: Encoder class
        :param folds: Folds to split the data

        """

        self.cols = cols
        self.encoder = encoder
        self.encoders_dict = {}
        self.folds = folds

    def fit_transform(self, X: pd.DataFrame, y: np.array) -> pd.DataFrame:

        X = X.reset_index(drop=True)
        y = y.reset_index(drop=True)
        for n_fold, (train_idx, val_idx) in enumerate(self.folds.split(X, y)):
            X_train, X_val = X.loc[train_idx].reset_index(drop=True), X.loc[val_idx].reset_index(drop=True)
            y_train, y_val = y[train_idx], y[val_idx]
            _ = self.encoder.fit_transform(X_train, y_train)

            # transform validation part and get all necessary cols
            val_t = self.encoder.transform(X_val)

            if n_fold == 0:
                cols_representation = np.zeros((X.shape[0], val_t.shape[1]))
            
            self.encoders_dict[n_fold] = self.encoder

            cols_representation[val_idx, :] += val_t.values

        cols_representation = pd.DataFrame(cols_representation, columns=X.columns)

        return cols_representation

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:

        X = X.reset_index(drop=True)

        cols_representation = None

        for encoder in self.encoders_dict.values():
            test_tr = encoder.transform(X)

            if cols_representation is None:
                cols_representation = np.zeros(test_tr.shape)

            cols_representation = cols_representation + test_tr / self.folds.n_splits

        cols_representation = pd.DataFrame(cols_representation, columns=X.columns)
        
        return cols_representation