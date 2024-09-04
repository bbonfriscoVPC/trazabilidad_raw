import pandas as pd
import openpyxl
from typing import Any
import numpy as np


    
def tito():
    """
    
    """
    def convert_to_float(value: Any)->float:
        if isinstance(value, str):
            value = value.strip()
            if value != '' and value!='Sin Informacion':
                try:
                    return float(value.replace(',', '.', 1))
                except ValueError:
                    print(f'{value}, ValueError')
                    return np.nan
            else:
                return np.nan
        elif isinstance(value, (int, float)):
            return float(value)
        else:
            return np.nan

    mp=pd.read_csv('./mp.txt',
                delimiter=';',
                encoding="utf-8",
                dtype=str,
                low_memory=False)

    mgv=pd.read_csv('./mgv2.txt',
                delimiter=';',
                encoding="utf-8",
                dtype=str,
                low_memory=False)
    
    mgv_to_float=['VENTATOTAL','VV100','REEM_INSUMOS']
    for col in mgv_to_float:
        mgv[col]=mgv[col].apply(convert_to_float)
    mp_to_float=['CAJAS_CONSUMIDAS','CAJAS_SALDOS','COSVTA_IRFS']
    for col in mp_to_float:
        mp[col]=mp[col].apply(convert_to_float)

    mgv_docref=mgv['DOCREF'].unique().tolist()
    mgv = mgv[~mgv['DOCUMENTO'].isin(mgv_docref)]
    mgv = mgv[~(mgv['VV100'] <= 0)]

    mgv=mgv[(mgv['ORIGEN'] != 'Importado')]

    ###
    #mgv=mgv[(mgv['LOTE'].isna())]
    #mgv=mgv[mgv['LOTE'].str.strip() != '']
    #mgv=mgv[(mgv['DOCUMENTO'].isna())]
    #mgv=mgv[mgv['DOCUMENTO'].str.strip() != '']  

    FILTRO_ANULA_TIPODOC=['Anula Factura','Nota debito','Nota credito','']
    mgv = mgv[~mgv['TIPODOC'].isin(FILTRO_ANULA_TIPODOC)]

    mgv_final=['DOCUMENTO','LOTE','ART_GENERICO','ORDEN_FABRICACION','ORIGEN','TIPODOC','FECHACONTA','VENTATOTAL','VV100','REEM_INSUMOS']
    #mgv=mgv[mgv_final]
    mp_final=['LOTE_ORDEN_FABRICACION','DOCUMENTO','NUMERO_ORDEN','CAJAS_CONSUMIDAS','CAJAS_SALDOS','COSVTA_IRFS','FECHA_DOCUMENTO','LOTE_COMPONENTE',
        'MATERIAL_COMPONENTE','TIPO_MATERIAL','TIPO_PROCESO','MATERIAL_ORDEN']
    #mp=mp[mp_final]

    mp.rename(columns={'LOTE_ORDEN_FABRICACION':'LOTE',
                       'MATERIAL_ORDEN':'ART_GENERICO'}
            ,inplace=True)

    df=mgv.merge(mp, on=['LOTE','DOCUMENTO','ART_GENERICO'], how='left',suffixes=('_MGV','_MP'))
    df = df[~(df['TIPO_MATERIAL'] == 'ZVER')]
    df['SUM_CAJAS_CONSUMIDAS'] = df.groupby(['DOCUMENTO','LOTE'])['CAJAS_CONSUMIDAS'].transform('sum') 

    df['FACTOR_CAJAS_CONSUMIDAS']= np.where(
                                    df['SUM_CAJAS_CONSUMIDAS'] != 0,
                                    df['CAJAS_CONSUMIDAS'] / df['SUM_CAJAS_CONSUMIDAS'],
                                    np.nan)

    df['COSVTA_IRFS_MAS_INSUMOS'] = df['COSVTA_IRFS'] + np.multiply(df['REEM_INSUMOS'], df['FACTOR_CAJAS_CONSUMIDAS'])
    df['PRECIO_TOTAL_LINEA_MP'] = np.multiply(df['VENTATOTAL'], df['FACTOR_CAJAS_CONSUMIDAS'])

    df['PRECIO_UNITARIO_POR_LINEA_MP']=np.where(
                                        df['CAJAS_CONSUMIDAS'] != 0,
                                        df['PRECIO_TOTAL_LINEA_MP'] / df['CAJAS_CONSUMIDAS'],
                                        np.nan)
    
    df['MARGEN_MP'] = np.subtract(df['PRECIO_TOTAL_LINEA_MP'], df['COSVTA_IRFS_MAS_INSUMOS'])

    df.to_csv('final_sin_importado.csv',sep=';',index=False)

    mismatched_rows = df[df['CAJAS_CONSUMIDAS'].isna()]
    happy_df=df[~df['CAJAS_CONSUMIDAS'].isna()]
    #happy_df.to_csv('happy_df.csv',sep=';',index=False)
    happy_df.to_excel('happy_df.xlsx',sheet_name='Sheet1',index=False)
    mismatched_rows.to_csv('missmatch.csv',sep=';',index=False)
    print('missmatch',mismatched_rows.shape)
    print('entero',df.shape)

if __name__=='__main__':
    """
    mgv=pd.read_csv('./mgv2.txt',
            delimiter=';',
            encoding="utf-8",
            dtype=str,
            low_memory=False)
    mgv.to_csv('mgv2.csv',sep=';',index=False)

    mp=pd.read_csv('./mp.txt',
            delimiter=';',
            encoding="utf-8",
            dtype=str,
            low_memory=False)
    mp.to_csv('mp.csv',sep=';',index=False)
    """
    tito()
    #agregar logica qlik de los docref que anulan una factura en el mgv
