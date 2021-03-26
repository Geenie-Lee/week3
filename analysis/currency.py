import FinanceDataReader as fdr
import pandas as pd
import matplotlib.pyplot as plt

df = fdr.DataReader('USD/KRW', '2021')
print(df)

plt.figure(figsize=(15, 7))
df['Close'].plot()

df2 = fdr.DataReader('KS11', '2021')
print(df2)

df_sum = pd.concat([df, df2])
print(df_sum)