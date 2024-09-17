from ucimlrepo import fetch_ucirepo
from IPython.display import display
import pandas as pd

# fetch dataset 
infrared_thermography_temperature = fetch_ucirepo(id=925) 
  
# data (as pandas dataframes) 
X = infrared_thermography_temperature.data.features 
y = infrared_thermography_temperature.data.targets 
  

# metadata 
display(infrared_thermography_temperature.metadata)
# variable information 
display(infrared_thermography_temperature.variables) 
display(X.head())
display(y.head())
display(X.dtypes)
#lets find numeric columns
numeric_columns = X.select_dtypes(include=['float64','int64']).columns
display(numeric_columns)
#now the categorical columns
categorical_columns = X.select_dtypes(include=['object']).columns
display(categorical_columns)
#since gender age and ethnicity are not numerical, we will encode them
X_encoded = pd.get_dummies(X, columns=['Gender','Ethnicity'], drop_first=True)

#age is a range, so we're going to extract the lower bounds
X_encoded['Age'] = X_encoded['Age'].str.extract('(\d+)').astype(int)

display(X_encoded)
#encoding the ethinicty and gender into binary


#['Gender_Male', 'Ethnicity_Asian','Ethnicity_Black or African-American','Ethnicity_Hispanic/Latino','Ethnicity_Multiracial','Ethnicity_White']

display(X_encoded)
#summarize stats
display(X_encoded.describe())
display(y.describe)

import seaborn as sns
import matplotlib.pyplot as plt

# Histograms for numerical features
X_encoded.hist(figsize=(15, 10))
plt.tight_layout()
plt.show()



# detect outliers
for column in numeric_columns:
    plt.figure(figsize=(6, 4))
    sns.boxplot(y=X[column])
    plt.title(f'Boxplot of {column}')
    plt.show()