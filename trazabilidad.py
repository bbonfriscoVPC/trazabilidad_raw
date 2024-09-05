import pandas as pd
import openpyxl
from typing import Any
import numpy as np

def tito():
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
    
    #######
    ###LOAD
    #######

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
    
    ################
    ###CAST TO FLOAT
    ################
        
    mgv_to_float=['VENTATOTAL','VV100','REEM_INSUMOS']
    for col in mgv_to_float:
        mgv[col]=mgv[col].apply(convert_to_float)
    mp_to_float=['CAJAS_CONSUMIDAS','CAJAS_SALDOS','COSVTA_IRFS']
    for col in mp_to_float:
        mp[col]=mp[col].apply(convert_to_float)

    ###############################################################################################
    ###ELIMINACION DE DOCUMENTOS REFERENCIADOS POR DOCREF (NOTA DE CREDITO, DEBITO Y ANULA FACTURA)
    ###############################################################################################

    list_docref=mgv['DOCREF'].unique().tolist()
    df_no_aplica_calculos =mgv[mgv['DOCUMENTO'].isin(list_docref)]
    df_no_aplica_calculos=df_no_aplica_calculos.assign(DESCARTADO_POR='ELIMINACION DE DOCUMENTOS REFERENCIADOS POR DOCREF (NOTA DE CREDITO, DEBITO Y ANULA FACTURA')
    mgv = mgv[~mgv['DOCUMENTO'].isin(list_docref)]

    ############################################################################
    ###ELIMINACION DE NOTAS DE CREDITOS ARTIFICIALES A TRAVES DE CAJAS NEGATIVAS
    ############################################################################
    
    aux=mgv[(mgv['VV100'] <= 0)]
    aux=aux.assign(DESCARTADO_POR='ELIMINACION DE NOTAS DE CREDITOS ARTIFICIALES A TRAVES DE CAJAS NEGATIVAS')
    df_no_aplica_calculos=pd.concat([df_no_aplica_calculos,aux],axis=0,ignore_index=True)
    mgv = mgv[~(mgv['VV100'] <= 0)]

    #########################################################################################
    ###ELIMINACION DE LOS DOCUMENTOS DIRECTOS QUE SON NOTA DE CREDITO, DEBITO Y ANULA FACTURA
    #########################################################################################

    FILTRO_ANULA_TIPODOC=['Anula Factura','Nota debito','Nota credito','']
    aux = mgv[mgv['TIPODOC'].isin(FILTRO_ANULA_TIPODOC)]
    aux=aux.assign(DESCARTADO_POR='ELIMINACION DE LOS DOCUMENTOS DIRECTOS QUE SON NOTA DE CREDITO, DEBITO Y ANULA FACTURA')
    df_no_aplica_calculos=pd.concat([df_no_aplica_calculos,aux],axis=0,ignore_index=True)
    mgv = mgv[~mgv['TIPODOC'].isin(FILTRO_ANULA_TIPODOC)]

    #######################################
    ###SEPERACION DE REEMPAQUE CON EL RESTO
    #######################################

    aux=mgv[(mgv['ORIGEN']!='Reempaque')]
    aux=aux.assign(DESCARTADO_POR='SEPERACION DE REEMPAQUE CON EL RESTO')
    df_no_aplica_calculos=pd.concat([df_no_aplica_calculos,aux],axis=0,ignore_index=True)
    mgv=mgv[(mgv['ORIGEN']=='Reempaque')]

    ##########################
    ###LIMPIADO DE INFORMACION
    ##########################

    aux=mgv[(mgv['LOTE'].isna())]
    aux=aux.assign(DESCARTADO_POR='LIMPIADO DE INFORMACION: LOTE NONE')
    df_no_aplica_calculos=pd.concat([df_no_aplica_calculos,aux],axis=0,ignore_index=True)

    aux=mgv[mgv['LOTE'].str.strip() == '']
    aux=aux.assign(DESCARTADO_POR='LIMPIADO DE INFORMACION: LOTE VACIO')
    df_no_aplica_calculos=pd.concat([df_no_aplica_calculos,aux],axis=0,ignore_index=True)

    mgv=mgv[~(mgv['LOTE'].isna())]
    mgv=mgv[mgv['LOTE'].str.strip() != '']

    aux=mgv[(mgv['DOCUMENTO'].isna())]
    aux=aux.assign(DESCARTADO_POR='LIMPIADO DE INFORMACION: DOCUMENTO NONE')
    df_no_aplica_calculos=pd.concat([df_no_aplica_calculos,aux],axis=0,ignore_index=True)
    
    aux=mgv[mgv['DOCUMENTO'].str.strip() == '']
    aux=aux.assign(DESCARTADO_POR='LIMPIADO DE INFORMACION: DOCUMENTO VACIO')
    df_no_aplica_calculos=pd.concat([df_no_aplica_calculos,aux],axis=0,ignore_index=True)

    mgv=mgv[~(mgv['DOCUMENTO'].isna())]
    mgv=mgv[mgv['DOCUMENTO'].str.strip() != '']

    aux=mgv[(mgv['ORDEN_FABRICACION'].isna())]
    aux=aux.assign(DESCARTADO_POR='LIMPIADO DE INFORMACION: ORDEN_FABRICACION NONE')
    df_no_aplica_calculos=pd.concat([df_no_aplica_calculos,aux],axis=0,ignore_index=True)  

    aux=mgv[mgv['ORDEN_FABRICACION'].str.strip() == '']
    aux=aux.assign(DESCARTADO_POR='LIMPIADO DE INFORMACION: ORDEN_FABRICACION VACIO')
    df_no_aplica_calculos=pd.concat([df_no_aplica_calculos,aux],axis=0,ignore_index=True)

    mgv=mgv[~(mgv['ORDEN_FABRICACION'].isna())]
    mgv=mgv[mgv['ORDEN_FABRICACION'].str.strip() != '']

    ##########
    ###CALIDAD
    ##########

    #mgv.to_excel('df_aplica_calculos.xlsx',sheet_name='Sheet1',index=False)
    #df_no_aplica_calculos.to_excel('df_no_aplica_calculos.xlsx',sheet_name='Sheet1',index=False)  

    ############
    ###CUT CLEAN
    ############

    mgv_final=['DOCUMENTO','LOTE','ART_GENERICO','ORDEN_FABRICACION','ORIGEN','TIPODOC','FECHACONTA','VENTATOTAL','VV100','REEM_INSUMOS']
    #mgv=mgv[mgv_final]
    mp_final=['LOTE_ORDEN_FABRICACION','DOCUMENTO','NUMERO_ORDEN','CAJAS_CONSUMIDAS','CAJAS_SALDOS','COSVTA_IRFS','FECHA_DOCUMENTO','LOTE_COMPONENTE',
        'MATERIAL_COMPONENTE','TIPO_MATERIAL','TIPO_PROCESO','MATERIAL_ORDEN']
    #mp=mp[mp_final]

    ########
    ###JOINS
    ########

    mp=mp.assign(ORIGEN_METADATA='MP')
    mp=mp.add_suffix('_mp')
    mgv=mgv.add_suffix('_mgv')
    mp.rename(columns={'LOTE_ORDEN_FABRICACION_mp':'LOTE',
                'MATERIAL_ORDEN_mp':'ART_GENERICO',
                'DOCUMENTO_mp':'DOCUMENTO',
                'NUMERO_ORDEN_mp':'ORDEN_FABRICACION'
                },inplace=True)
    mgv.rename(columns={'LOTE_mgv':'LOTE',
            'ART_GENERICO_mgv':'ART_GENERICO',
            'DOCUMENTO_mgv':'DOCUMENTO',
            'ORDEN_FABRICACION_mgv':'ORDEN_FABRICACION'
            },inplace=True)
    
    df=mgv.merge(mp, on=['LOTE','DOCUMENTO','ART_GENERICO','ORDEN_FABRICACION'], how='left')

    #df.to_excel('df_join.xlsx',sheet_name='Sheet1',index=False)

    ######################################################################################################
    ###ELIMINACION DE INSUMOS YA QUE LA COLUMNA REEM_INSUMOS DE MGV2 TRAE LA SUMA DE LOS COSTOS DE INSUMOS
    ######################################################################################################

    df = df[(df['TIPO_MATERIAL_mp'] != 'ZVER')]
    """
    KEY      |ORIGEN_METADATA|TIPO_MATERIAL|DOC|LOTE|MAT|VV100|CAJAS_CONSUMIDAS|LOGICA_NEGOCIO
    123456789|MGV            |VENTA        |123|456 |789|12   |0               |0
    123456789|MP             |ZHAW         |123|456 |789|0    |1               |1,2
    123456789|MP             |ZHAW         |123|456 |789|0    |3               |2
    123456789|MP             |ZHAW         |123|456 |789|0    |1               |3
    123456789|MP             |ZHAW         |123|456 |789|0    |1               |4
->  123456789|MP             |ZVER         |123|456 |789|0    |21              |0             
    """  
    ####################
    ###LOGICA DE NEGOCIO
    ####################

    df['SUM_CAJAS_CONSUMIDAS'] = df.groupby(['DOCUMENTO','LOTE'])['CAJAS_CONSUMIDAS_mp'].transform('sum') 

    df['FACTOR_CAJAS_CONSUMIDAS']= np.where(
                                    df['SUM_CAJAS_CONSUMIDAS'] != 0,
                                    df['CAJAS_CONSUMIDAS_mp'] / df['SUM_CAJAS_CONSUMIDAS'],
                                    np.nan)

    df['COSVTA_IRFS_MAS_INSUMOS_POR_LINEA_MP'] = df['COSVTA_IRFS_mp'] + np.multiply(df['REEM_INSUMOS_mgv'], df['FACTOR_CAJAS_CONSUMIDAS'])
    df['PRECIO_TOTAL_POR_LINEA_MP'] = np.multiply(df['VENTATOTAL_mgv'], df['FACTOR_CAJAS_CONSUMIDAS'])

    df['PRECIO_UNITARIO_POR_LINEA_MP']=np.where(
                                        df['CAJAS_CONSUMIDAS_mp'] != 0,
                                        df['PRECIO_TOTAL_POR_LINEA_MP'] / df['CAJAS_CONSUMIDAS_mp'],
                                        np.nan)
    
    df['MARGEN_POR_LINEA_MP'] = np.subtract(df['PRECIO_TOTAL_POR_LINEA_MP'], df['COSVTA_IRFS_MAS_INSUMOS_POR_LINEA_MP'])

    mismatched_rows = df[df['CAJAS_CONSUMIDAS_mp'].isna()]

    print('missmatch',mismatched_rows.shape)
    print('entero',df.shape)
    #df.to_excel('final.xlsx',sheet_name='Shee1',index=False)

    #######################################
    ###LOGICA PARA LAS AGREGACIONES DE QLIK
    #######################################

    df.rename(columns={'ORIGEN_METADATA_mp':'ORIGEN_METADATA'
                       },inplace=True)
    
    columns_mgv = [col for col in df.columns if col.endswith('_mgv')]
    columns_mp = [col for col in df.columns if col.endswith('_mp')]

    #mask = df['ORIGEN_METADATA'] == 'MP'
    #df.loc[mask, columns_mgv] = None
    df[columns_mgv] = None
    df=pd.concat([df,mgv],axis=0,ignore_index=True)
    final=['DOCUMENTO','LOTE','ART_GENERICO','ORDEN_FABRICACION','ORIGEN_METADATA','ORIGEN_mgv','TIPODOC_mgv','FECHACONTA_mgv','VENTATOTAL_mgv','VV100_mgv',
     'REEM_INSUMOS_mgv','CAJAS_CONSUMIDAS_mp','CAJAS_SALDOS_mp','COSVTA_IRFS_mp','FECHA_DOCUMENTO_mp','LOTE_COMPONENTE_mp',
     'MATERIAL_COMPONENTE_mp','TIPO_MATERIAL_mp','TIPO_PROCESO_mp','SUM_CAJAS_CONSUMIDAS','FACTOR_CAJAS_CONSUMIDAS',
    'COSVTA_IRFS_MAS_INSUMOS_POR_LINEA_MP','PRECIO_TOTAL_POR_LINEA_MP','PRECIO_UNITARIO_POR_LINEA_MP','MARGEN_POR_LINEA_MP']
    #df=df[final]
    print(df.shape)
    df.to_excel('for_qlik.xlsx',sheet_name='Sheet1',index=False)

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
    #chequear si hay una nota de credito,debito o anulacion de factura con un origen distinto a reempaque que afecte una factura de origen rempaque
