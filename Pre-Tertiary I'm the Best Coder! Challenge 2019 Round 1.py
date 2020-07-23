#!/usr/bin/env python
# coding: utf-8

# In[1]:
import pandas as pd 
#digunakan untuk mengimport library pandas sebagai pd

# In[2]:
extra2=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200530 Top Performing Brands in Shopee\2. Prepared Data\Extra_material_2.csv')
extra3=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200530 Top Performing Brands in Shopee\2. Prepared Data\Extra_material_3.csv')
#read_csv digunakan untuk mengimport file csv dengan full path nya

# In[3]:
gross_sales_revenue=extra3.amount*extra3.item_price_usd
#diciptakan sebuah series hasil perkalian kolom amount dan kolom item_price_usd pada tabel extra3 dengan series bernama gross_sales_revenue
# In[4]:
extra3 = extra3.rename(columns={'shopid':'shop_id'})
extra3['gross_sales_revenue']=pd.Series(gross_sales_revenue)
#mengganti nama kolom shopid menjadi shop_id pada table extra3 agar sama dengan kolom pada table extra 2
#memasukkan series gross_sales_revenue pada table extra3
# In[5]:
extra3['date_id'] = pd.to_datetime(extra3['date_id'], format='%d/%m/%Y')
extra3_1=extra3[(extra3.date_id>='05/10/2019') & (extra3.date_id<='31/10/2019')]
extra3_2=extra3_1.reset_index()
del extra3_2['index']
#menformat tanggal pada table 3 kolom date_id menjadi format tipe tanggal
#menfilter tanggal dari range 5 oktober 2019 sampai 31 oktober 2019
#sesuaikan kembali index
# In[6]:
extra2.shop_id=extra2.shop_id.astype(int).astype(str)
extra3_2.shop_id=extra3_2.shop_id.astype(int).astype(str)
combine=pd.merge(left=extra2,right=extra3_2,how='left',left_on='shop_id',right_on='shop_id')
#pastikan shop_id pada kedua kolom mempunyai tipe object yang sama dan gabungkan kedua tabel dengan shop_id sebagai referensi

# In[7]:
combine_1=combine.copy()
#digunakan untuk menjaga jaga jika terjadi kesalahan pada combine_1

# In[8]:
combine_2=combine_1.groupby(['brand','itemid']).gross_sales_revenue.agg(['sum'])
#kelompokkan data pada table combine_1 berdasarkan kolom brand dan itemid dengan penjumlahan kolom gross_sales_revenue

# In[9]:
combine_3=combine_2.sort_values(by=['brand','sum'],ascending=[True,False])
#urutkan  tabel combine_2 dengan prioritas brand dari kecil ke besar terlebih dahulu dan sum dari besar ke kecil

# In[10]:
combine_4=combine_3.reset_index()
#sesuaikan kembali index

# In[11]:
combine_5=combine_4.groupby(['brand']).head(3)
#kelompokkan kembali sesuai dengan brand dan ambil 3 data dengan nilai sum terbesar tiap kelompoknya

# In[12]:
combine_6=combine_5.reset_index()
combine_7= combine_6.copy()
del combine_7['index']
#sesuaikan index kembali

# In[13]:
brand=pd.Series(extra2.brand)
combine_8=pd.merge(left=combine_7,right=brand,how='right',left_on='brand',right_on='brand')
#buat sebuah series bernama brand yang berisikan kolom brand tabri tabel extra 2 kolom brand
#gabungkan tabel pada combine_7 dengan series brand dengan referensi kolom brand
# In[14]:


combine_8


# In[15]:


combine_9 = combine_8.drop_duplicates(subset=['brand','itemid', 'sum'], keep='first')
#memastikan tidak ada nilai duplikat

# In[16]:
combine_9.itemid=combine_9.itemid.fillna(0)
combine_9.itemid=combine_9.itemid.astype(int)
combine_9.itemid=combine_9.itemid.astype(str)
combine_9.itemid=combine_9.itemid.replace('0','N.A')
#perubahan tipe beberapa kali pada itemid untuk mengihilangkan 'NaN' dan merubahnya menjadi string 'N.A'

# In[17]:


combine_10=combine_9.groupby('brand')['itemid'].apply(', '.join).reset_index()
combine_10['Answers'] = combine_10['brand'].str.cat(combine_10['itemid'],sep=", ")
#kelompokkan 3 kolom dengan itemid sum terbesar tiap kelompoknya dalam satu kolom yang sama dan dipisahkan dengan ','
#gabungkan brand dengan itemid dengan 3 sum terbesar pada kolom yang sama 
# In[18]:


del combine_10['brand']
del combine_10['itemid']
#habus kolom brand dan itemid agar hanya terdapat Answer

# In[19]:


combine_10.index=combine_10.index+1
#membuat kolom baru bernama index dan index bermulai dari angka 1

# In[20]:


combine_10.to_csv('submission of competition answers.csv',index_col = False)
#extraxt file tabel menjadi csv tanpa index. jadi hasil berubah 2 kolom bernama index dan answer
