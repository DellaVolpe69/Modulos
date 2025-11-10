import pandas as pd
from pathlib import Path, PureWindowsPath

df_aux = pd.read_parquet(PureWindowsPath(r"\\tableau\Central_de_Performance\BI\Local\Bases_Tratadas\Bases_Auxiliares\IBGE.parquet").as_posix())
columns = df_aux.columns

def trataPrazo(df, colunaCid, colunaUF, ref='REFERENCIA_MEDICAO', ligacao='left_only'):
    # Separar linhas com cidade nula (Todo o Estado)
    df_estado = df[df[colunaCid].isna()].copy()
    df_existentes = df[df[colunaCid].notna()][[ref, colunaUF, colunaCid]].drop_duplicates()

    # Expandir para todas as cidades do estado
    df_expandido = df_estado.merge(df_aux, left_on=colunaUF, right_on='UF', how="left")
    df_expandido[colunaCid] = df_expandido['NOME_MUNICIPIO']

    # Remover combinações já existentes
    df_expandido = df_expandido.merge(
        df_existentes,
        how='left',
        left_on=[ref, colunaUF, colunaCid],
        right_on=[ref, colunaUF, colunaCid],
        indicator=True
    )
    df_expandido = df_expandido[df_expandido['_merge'].isin([ligacao])].drop(columns=['_merge'])

    # Limpeza
    df_expandido = df_expandido.drop(columns=columns).drop_duplicates().reset_index(drop=True)

    return df_expandido

def trata(df):
    df_paridade = trataPrazo(df, 'COLETA_CIDORIGEM', 'COLETA_UFORIGEM', ligacao='both')
    df_paridade = df_paridade.loc[df_paridade['COLETA_CIDORIGEM'] == df_paridade['COLETA_CIDDESTINO']]

    df_agregar = trataPrazo(df, 'COLETA_CIDORIGEM', 'COLETA_UFORIGEM', ligacao='left_only')
    df_agregar1 = trataPrazo(df, 'COLETA_CIDDESTINO', 'COLETA_UFDESTINO', ligacao='left_only')

    df = pd.concat([df, df_agregar, df_agregar1, df_paridade], ignore_index=True).drop_duplicates(['REFERENCIA_MEDICAO', 'INCOTERMS', 'COLETA_CIDORIGEM', 'COLETA_UFORIGEM', 'COLETA_CIDDESTINO', 'COLETA_UFDESTINO']).reset_index(drop=True)
    #df = df[(df["COLETA_CIDORIGEM"].notna()) & (df["COLETA_CIDDESTINO"].notna())]
    df = df[(~((df["COLETA_CIDORIGEM"].notna() & df["COLETA_CIDDESTINO"].isna()))) & (~((df["COLETA_CIDORIGEM"].isna() & df["COLETA_CIDDESTINO"].notna())))]

    return df
