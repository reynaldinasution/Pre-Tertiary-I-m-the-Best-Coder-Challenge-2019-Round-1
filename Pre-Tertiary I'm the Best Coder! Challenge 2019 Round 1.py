#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


extra2=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200530 Top Performing Brands in Shopee\2. Prepared Data\Extra_material_2.csv')
extra3=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200530 Top Performing Brands in Shopee\2. Prepared Data\Extra_material_3.csv')


# In[3]:


gross_sales_revenue=extra3.amount*extra3.item_price_usd


# In[4]:


extra3 = extra3.rename(columns={'shopid':'shop_id'})
extra3['gross_sales_revenue']=pd.Series(gross_sales_revenue)


# In[5]:


extra3['date_id'] = pd.to_datetime(extra3['date_id'], format='%d/%m/%Y')
extra3_1=extra3[(extra3.date_id>='05/10/2019') & (extra3.date_id<='31/10/2019')]
extra3_2=extra3_1.reset_index()
del extra3_2['index']


# In[6]:


extra2.shop_id=extra2.shop_id.astype(int).astype(str)
extra3_2.shop_id=extra3_2.shop_id.astype(int).astype(str)
combine=pd.merge(left=extra2,right=extra3_2,how='left',left_on='shop_id',right_on='shop_id')


# In[7]:


combine_1=combine


# In[8]:


combine_2=combine_1.groupby(['brand','itemid']).gross_sales_revenue.agg(['sum'])


# In[9]:


combine_3=combine_2.sort_values(by=['brand','sum'],ascending=[True,False])


# In[10]:


combine_4=combine_3.reset_index()


# In[11]:


combine_5=combine_4.groupby(['brand']).head(3)


# In[12]:


combine_6=combine_5.reset_index()
combine_7= combine_6
del combine_7['index']


# In[13]:


brand=pd.Series(extra2.brand)
combine_8=pd.merge(left=combine_7,right=brand,how='right',left_on='brand',right_on='brand')


# In[14]:


combine_8


# In[15]:


combine_9 = combine_8.drop_duplicates(subset=['brand','itemid', 'sum'], keep='first')


# In[16]:


combine_9.itemid=combine_9.itemid.fillna(0)
combine_9.itemid=combine_9.itemid.astype(int)
combine_9.itemid=combine_9.itemid.astype(str)
combine_9.itemid=combine_9.itemid.replace('0','N.A')


# In[17]:


combine_10=combine_9.groupby('brand')['itemid'].apply(', '.join).reset_index()
combine_10['Answers'] = combine_10['brand'].str.cat(combine_10['itemid'],sep=", ")


# In[18]:


del combine_10['brand']
del combine_10['itemid']


# In[19]:


combine_10.index=combine_10.index+1


# In[20]:


combine_10.to_csv('submission of competition answers.csv')

