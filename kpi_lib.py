from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def build_kpi(spark: SparkSession, periodo: str = None) -> DataFrame:
    """Auto-generated helper function extracted from FACT_KPI_NOVA.ipynb
    It attempts to reproduce the transformations and returns the final DataFrame.
    Review and adapt the code where necessary before using in production."""

    from pyspark.sql.functions import lit, trim, initcap, upper, when,length, col, concat, lpad, row_number,date_add, sum, current_date, expr, round, datediff, date_sub, count, countDistinct, avg, max, year, month, min, date_trunc, lag, first, last
    
    from datetime import date, datetime, timedelta
    
    from pyspark.sql.types import TimestampType, IntegerType, DecimalType, LongType, DoubleType
    
    from delta import DeltaTable
    
    from pytz import timezone
    
    from pyspark.sql import Window

    currdate = date.today()
    data_final = (currdate).strftime('%Y-%m-%d')

    sale= sql(" SELECT ORIGEM,  CPF,  MARCA,   DATA,  VLF,  QLF,  TICKET,  TIPO_VENDA, CPV FROM gold_clientes.view_vendas_captadas_base_ativa_com_franquia")
    
    purchase = sql("SELECT CPF, MARCA, YEAR(DATA_COMPRA) AS ANO, MONTH(DATA_COMPRA) AS MES, RECORRENCIA, DATA_COMPRA FROM GOLD_CLIENTES.FACT_PURCHASE ") 
    
    dim_calendario = sql(f"select distinct ANO, MES, '-' as suporte from gold.dim_calendario where DATA < '{data_final}'")
    
    #adicionei
    part_eventos = sql('''
                       SELECT
                        X.MARCA, Y.CPF, year(Y.DATA_VENDA) as ANO, month(Y.DATA_VENDA) as MES, COUNT (distinct X.TIPO_EVENTO) AS PART_EVENTOS
                        FROM gold_clientes.fact_eventos_loja X
                        INNER JOIN (
                          SELECT CPF, TIPO_OPERACAO , MARCA, TICKET, CODIGO_FILIAL, DATA_VENDA, VALOR_LIQUIDO_POS 
                          FROM GOLD.fact_comission_store 
                          WHERE TIPO_OPERACAO IN ('Normal', 'Venda Prateleira Infinita','Venda Cupom Desconto','Venda Mobile Checkout')) Y 
                          on X.CODIGO_FILIAL == Y.CODIGO_FILIAL and X.MARCA=Y.MARCA and Y.DATA_VENDA = X.DATA_INICIAL 
                        WHERE Y.CPF <> 'null'
                        GROUP BY 1,2,3,4
                       ''')
    
    base_influenciada = sql('''
                            with base_influenciada as (
                              select MARCA, sms.CPF,sms.TICKET, sms.DATA_VENDA, 'SMS' as tipo
                              from gold_clientes.fact_venda_influenciada_sms sms
                              union
                              select MARCA, e.CPF,e.TICKET, e.DATA_VENDA, 'EMAIL' as tipo
                              from gold_clientes.fact_venda_influenciada_email e
                              union
                              select MARCA_VENDA AS MARCA, tl.CPF,tl.TICKET, tl.DATA_VENDA, 'TLMKT' as tipo
                              from gold_lojas.fact_venda_influenciada_tlmkt tl
                            )
                            select b.MARCA, year(DATA_VENDA) as ANO, month(DATA_VENDA) as MES, b.CPF,
                              count(distinct case when tipo='TLMKT' then DATA_VENDA end) as QTD_TLMKT,
                              count(distinct case when tipo='SMS' then DATA_VENDA end) as QTD_SMS,
                              count(distinct case when tipo='EMAIL' then DATA_VENDA end) as QTD_EMAIL
                            from base_influenciada b
                            group by 1,2,3,4
                            ''')

    valor = sale
    
    #novas colunas
    valor = valor.withColumn('ANO', year('DATA'))
    valor = valor.withColumn('MES', month('DATA'))
    valor = valor.withColumn('ROL', col('VLF')* lit(0.744))
    #valor = valor.withColumn('CPV', col('QLF')* col('CUSTO_UNITARIO'))
    valor = valor.withColumn('LUCRO', col('ROL') - col('CPV'))
    valor = valor.withColumn('MARGEM', col('LUCRO') / col('VLF'))
    
    
    #TEMPO
    tempo = valor.select('MARCA')
    tempo = tempo.withColumn('suporte', lit('-')).distinct()
    
    tempo = tempo.join(dim_calendario, on=['suporte'], how = 'inner')
    
    
    #CLIENTE
    cliente = valor.select('CPF', 'MARCA').distinct()
    
    
    #PRI_VENDA
    pri_venda = valor.select('CPF', 'MARCA', 'DATA')
    pri_venda = pri_venda.groupBy('CPF', 'MARCA').agg(min(col('DATA')).alias('DT_PRI_VENDA'))
    
    
    #agregacoes no dataframe VALOR
    valor = valor.groupBy('CPF', 'MARCA', 'ANO', 'MES').agg(sum(col('VLF')).alias('VLF'),\
                                                            sum(when(col('ORIGEM').isin('Sale'), col('VLF')).otherwise(lit(0))).alias('VLF_OFF'),\
                                                            sum(when(col('ORIGEM').isin('Franquia'), col('VLF')).otherwise(lit(0))).alias('VLF_FRANQUIA'),\
                                                            sum(when(col('ORIGEM') == 'Ecom', col('VLF')).otherwise(lit(0))).alias('VLF_ON'),\
                                                            sum(when(col('TIPO_VENDA') == 'FP', col('VLF')).otherwise(lit(0))).alias('VLF_FP'), \
                                                            sum(when(col('TIPO_VENDA') == 'MD', col('VLF')).otherwise(lit(0))).alias('VLF_MD'), \
                                                            sum(when(col('TIPO_VENDA') == 'EC', col('VLF')).otherwise(lit(0))).alias('VLF_EC'), \
                                                            sum(col('QLF')).alias('QLF'), \
                                                            sum(when(col('TIPO_VENDA') == 'FP', col('QLF')).otherwise(lit(0))).alias('QLF_FP'), \
                                                            sum(when(col('TIPO_VENDA') == 'MD', col('QLF')).otherwise(lit(0))).alias('QLF_MD'), \
                                                            sum(when(col('TIPO_VENDA') == 'EC', col('QLF')).otherwise(lit(0))).alias('QLF_EC'), \
                                                            countDistinct(concat(col('TICKET'), col('DATA'))).alias('QT_TICKET'), \
                                                            countDistinct(when(col('ORIGEM').isin('Sale'), concat(col('TICKET'), col('DATA')))).alias('QT_TICKET_OFF'), \
                                                            countDistinct(when(col('ORIGEM').isin('Franquia'), concat(col('TICKET'), col('DATA')))).alias('QT_TICKET_FRANQUIA'), \
                                                            countDistinct(when(col('ORIGEM') == 'Ecom', concat(col('TICKET'), col('DATA')))).alias('QT_TICKET_ON'),\
                                                            (sum('VLF')/countDistinct(col('TICKET'))).alias('TICKET_MEDIO'),\
                                                            (sum('VLF')/sum('QLF')).alias('PRECO_MEDIO'), \
                                                            (sum('QLF')/countDistinct(col('TICKET'))).alias('PA'), \
                                                            sum(col('LUCRO')).alias('LUCRO'), \
                                                            sum(col('MARGEM')).alias('MARGEM')).distinct()

    # juntando com a part_eventos
    valor = valor.join(part_eventos, on = ['CPF','MARCA','ANO','MES'], how = 'left')
    valor = valor.join(base_influenciada, on = ['CPF','MARCA','ANO','MES'], how = 'left')

    #DIMENSAO
    #join TEMPO e CLIENTE
    dimensao = tempo.join(cliente, on = ['MARCA'], how = 'fullouter').distinct()
    
    #join PRI_VENDA
    dimensao = dimensao.join(pri_venda, on = ['CPF', 'MARCA'], how = 'left')
    
    #filtro
    dimensao = dimensao.filter(concat(col('ANO'), lit('-'), col('MES')) >= date_trunc("Month", col('DT_PRI_VENDA'))).drop('DT_PRI_VENDA')
    
    #join VALOR
    dimensao = dimensao.join(valor, on = ['CPF','MARCA', 'ANO', 'MES'], how = 'left')
    
    
    #Tratativas
    dimensao = dimensao.filter(col('CPF').isNotNull())
    dimensao = dimensao.withColumn('VLF_FP', when(col('VLF_FP').isNull(), 0).otherwise(col('VLF_FP')))
    dimensao = dimensao.withColumn('VLF', when(col('VLF').isNull(), 0).otherwise(col('VLF')))
    dimensao = dimensao.withColumn('VLF_OFF', when(col('VLF_OFF').isNull(), 0).otherwise(col('VLF_OFF')))
    dimensao = dimensao.withColumn('VLF_FRANQUIA', when(col('VLF_FRANQUIA').isNull(), 0).otherwise(col('VLF_FRANQUIA')))
    dimensao = dimensao.withColumn('VLF_ON', when(col('VLF_ON').isNull(), 0).otherwise(col('VLF_ON')))
    dimensao = dimensao.withColumn('VLF_MD', when(col('VLF_MD').isNull(), 0).otherwise(col('VLF_MD')))
    dimensao = dimensao.withColumn('VLF_EC', when(col('VLF_EC').isNull(), 0).otherwise(col('VLF_EC')))
    dimensao = dimensao.withColumn('QLF', when(col('QLF').isNull(), 0).otherwise(col('QLF')))
    dimensao = dimensao.withColumn('QLF_FP', when(col('QLF_FP').isNull(), 0).otherwise(col('QLF_FP')))
    dimensao = dimensao.withColumn('QLF_MD', when(col('QLF_MD').isNull(), 0).otherwise(col('QLF_MD')))
    dimensao = dimensao.withColumn('QLF_EC', when(col('QLF_EC').isNull(), 0).otherwise(col('QLF_EC')))
    dimensao = dimensao.withColumn('LUCRO', when(col('LUCRO').isNull(), 0).otherwise(col('LUCRO')))
    dimensao = dimensao.withColumn('MARGEM', when(col('MARGEM').isNull(), 0).otherwise(col('MARGEM')))
    dimensao = dimensao.withColumn('TICKET_MEDIO', when(col('TICKET_MEDIO').isNull(), 0).otherwise(col('TICKET_MEDIO')))
    dimensao = dimensao.withColumn('PRECO_MEDIO', when(col('PRECO_MEDIO').isNull(), 0).otherwise(col('PRECO_MEDIO')))
    dimensao = dimensao.withColumn('PA', when(col('PA').isNull(), 0).otherwise(col('PA')))
    dimensao = dimensao.withColumn('QT_TICKET', when(col('QT_TICKET').isNull(), 0).otherwise(col('QT_TICKET')))
    dimensao = dimensao.withColumn('QT_TICKET_OFF', when(col('QT_TICKET_OFF').isNull(), 0).otherwise(col('QT_TICKET_OFF')))
    dimensao = dimensao.withColumn('QT_TICKET_FRANQUIA', when(col('QT_TICKET_FRANQUIA').isNull(), 0).otherwise(col('QT_TICKET_FRANQUIA')))
    dimensao = dimensao.withColumn('QT_TICKET_ON', when(col('QT_TICKET_ON').isNull(), 0).otherwise(col('QT_TICKET_ON')))
    
    #adicionei
    dimensao = dimensao.withColumn('PART_EVENTOS', when(col('PART_EVENTOS').isNull(), 0).otherwise(col('PART_EVENTOS')))
    dimensao = dimensao.withColumn('QTD_TLMKT', when(col('QTD_TLMKT').isNull(), 0).otherwise(col('QTD_TLMKT')))
    dimensao = dimensao.withColumn('QTD_SMS', when(col('QTD_SMS').isNull(), 0).otherwise(col('QTD_SMS')))
    dimensao = dimensao.withColumn('QTD_EMAIL', when(col('QTD_EMAIL').isNull(), 0).otherwise(col('QTD_EMAIL')))
    
    #novas colunas
    dimensao = dimensao.withColumn('PER_PECAS_REMARC', col('QLF_MD') / col('QLF'))
    dimensao = dimensao.withColumn('FREQUENCIA', col('QT_TICKET'))
    
    #conversão
    dimensao = dimensao.withColumn('PER_PECAS_REMARC', when(col('PER_PECAS_REMARC').isNull(), 0).otherwise(col('PER_PECAS_REMARC')))
    
    #ordenacao
    dimensao = dimensao.orderBy('MARCA', 'ANO', 'MES')
    
    
    #PROCESSO DO ACUMULADO
    from pyspark.sql import functions as F
    
    particao = (Window.partitionBy('CPF', 'MARCA').orderBy(col('ANO'), col('MES')).rowsBetween(-11, 0))
    
    
    dimensao = dimensao.withColumn('VLF_ACUMULADO', F.sum('VLF').over(particao))
    dimensao = dimensao.withColumn('VLF_OFF_ACUMULADO', F.sum('VLF_OFF').over(particao))
    dimensao = dimensao.withColumn('VLF_FRANQUIA_ACUMULADO', F.sum('VLF_FRANQUIA').over(particao))
    dimensao = dimensao.withColumn('VLF_ON_ACUMULADO', F.sum('VLF_ON').over(particao))
    dimensao = dimensao.withColumn('VLF_FP_ACUMULADO', F.sum('VLF_FP').over(particao))
    dimensao = dimensao.withColumn('VLF_MD_ACUMULADO', F.sum('VLF_MD').over(particao))
    dimensao = dimensao.withColumn('VLF_EC_ACUMULADO', F.sum('VLF_EC').over(particao))
    dimensao = dimensao.withColumn('QLF_ACUMULADO', F.sum('QLF').over(particao))
    dimensao = dimensao.withColumn('QLF_FP_ACUMULADO', F.sum('QLF_FP').over(particao))
    dimensao = dimensao.withColumn('QLF_MD_ACUMULADO', F.sum('QLF_MD').over(particao))
    dimensao = dimensao.withColumn('QLF_EC_ACUMULADO', F.sum('QLF_EC').over(particao))
    dimensao = dimensao.withColumn('LUCRO_ACUMULADO', F.sum('LUCRO').over(particao))
    dimensao = dimensao.withColumn('MARGEM_ACUMULADO', F.sum('MARGEM').over(particao))
    dimensao = dimensao.withColumn('QT_TICKET_ACUMULADO', F.sum('QT_TICKET').over(particao))
    dimensao = dimensao.withColumn('QT_TICKET_OFF_ACUMULADO', F.sum('QT_TICKET_OFF').over(particao))
    dimensao = dimensao.withColumn('QT_TICKET_FRANQUIA_ACUMULADO', F.sum('QT_TICKET_FRANQUIA').over(particao))
    dimensao = dimensao.withColumn('QT_TICKET_ON_ACUMULADO', F.sum('QT_TICKET_ON').over(particao))
    #adicionei
    dimensao = dimensao.withColumn('PART_EVENTOS_ACUMULADO', F.sum('PART_EVENTOS').over(particao))
    dimensao = dimensao.withColumn('QTD_TLMKT_ACUMULADO', F.sum('QTD_TLMKT').over(particao))
    dimensao = dimensao.withColumn('QTD_SMS_ACUMULADO', F.sum('QTD_SMS').over(particao))
    dimensao = dimensao.withColumn('QTD_EMAIL_ACUMULADO', F.sum('QTD_EMAIL').over(particao))
    
    dimensao = dimensao.withColumn('TICKET_MEDIO_ACUMULADO', col('VLF_ACUMULADO')/col('QT_TICKET_ACUMULADO'))
    dimensao = dimensao.withColumn('PRECO_MEDIO_ACUMULADO', col('VLF_ACUMULADO')/col('QLF_ACUMULADO'))
    dimensao = dimensao.withColumn('FREQUENCIA_ACUMULADO', col('QT_TICKET_ACUMULADO')/12)
    dimensao = dimensao.withColumn('PER_PECAS_REMARC_ACUMULADO', col('QLF_MD_ACUMULADO') / col('QLF_ACUMULADO'))
    dimensao = dimensao.withColumn('PA_ACUMULADO',  col('QLF_ACUMULADO') /col('QT_TICKET_ACUMULADO'))
    
    dimensao = dimensao.withColumn('TICKET_MEDIO_ACUMULADO', when(col('TICKET_MEDIO_ACUMULADO').isNull(), 0).otherwise(col('TICKET_MEDIO_ACUMULADO')))
    dimensao = dimensao.withColumn('PRECO_MEDIO_ACUMULADO', when(col('PRECO_MEDIO_ACUMULADO').isNull(), 0).otherwise(col('PRECO_MEDIO_ACUMULADO')))
    dimensao = dimensao.withColumn('PER_PECAS_REMARC_ACUMULADO', when(col('PER_PECAS_REMARC_ACUMULADO').isNull(), 0).otherwise(col('PER_PECAS_REMARC_ACUMULADO')))
    dimensao = dimensao.withColumn('PA_ACUMULADO', when(col('PA_ACUMULADO').isNull(), 0).otherwise(col('PA_ACUMULADO')))
    
    
    #conversão
    dimensao= dimensao.withColumn('TICKET_MEDIO', dimensao['TICKET_MEDIO'].cast(DecimalType(14,2)))\
                      .withColumn('PRECO_MEDIO', dimensao['PRECO_MEDIO'].cast(DecimalType(14,2)))\
                      .withColumn('VLF', dimensao['VLF'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_OFF', dimensao['VLF_OFF'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_ON', dimensao['VLF_ON'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_FRANQUIA', dimensao['VLF_FRANQUIA'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_FP', dimensao['VLF_FP'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_MD', dimensao['VLF_MD'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_EC', dimensao['VLF_EC'].cast(DecimalType(14,2)))\
                      .withColumn('LUCRO', dimensao['LUCRO'].cast(DecimalType(14,2)))\
                      .withColumn('MARGEM', dimensao['MARGEM'].cast(DecimalType(14,2)))\
                      .withColumn('QLF', dimensao['QLF'].cast(LongType()))\
                      .withColumn('QLF_FP', dimensao['QLF_FP'].cast(LongType()))\
                      .withColumn('QLF_MD', dimensao['QLF_MD'].cast(LongType()))\
                      .withColumn('QLF_EC', dimensao['QLF_EC'].cast(LongType()))\
                      .withColumn('QT_TICKET', dimensao['QT_TICKET'].cast(LongType()))\
                      .withColumn('QT_TICKET_OFF', dimensao['QT_TICKET_OFF'].cast(LongType()))\
                      .withColumn('QT_TICKET_FRANQUIA', dimensao['QT_TICKET_FRANQUIA'].cast(LongType()))\
                      .withColumn('QT_TICKET_ON', dimensao['QT_TICKET_ON'].cast(LongType()))\
                      .withColumn('FREQUENCIA', dimensao['FREQUENCIA'].cast(LongType()))\
                      .withColumn('VLF_ACUMULADO', dimensao['VLF_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_OFF_ACUMULADO', dimensao['VLF_OFF_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_ON_ACUMULADO', dimensao['VLF_ON_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_FRANQUIA_ACUMULADO', dimensao['VLF_FRANQUIA_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_FP_ACUMULADO', dimensao['VLF_FP_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_MD_ACUMULADO', dimensao['VLF_MD_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('VLF_EC_ACUMULADO', dimensao['VLF_EC_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('LUCRO_ACUMULADO', dimensao['LUCRO_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('MARGEM_ACUMULADO', dimensao['MARGEM_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('TICKET_MEDIO_ACUMULADO', dimensao['TICKET_MEDIO_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('PRECO_MEDIO_ACUMULADO', dimensao['PRECO_MEDIO_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('FREQUENCIA_ACUMULADO', dimensao['FREQUENCIA_ACUMULADO'].cast(DecimalType(14,2)))\
                      .withColumn('PER_PECAS_REMARC_ACUMULADO', dimensao['PER_PECAS_REMARC_ACUMULADO'].cast(DoubleType()))\
                      .withColumn('PA_ACUMULADO', dimensao['PA_ACUMULADO'].cast(DoubleType()))
    
    #arredondamento
    dimensao = dimensao.withColumn('PER_PECAS_REMARC', round(col('PER_PECAS_REMARC'), 2))
    dimensao = dimensao.withColumn('PER_PECAS_REMARC_ACUMULADO', round(col('PER_PECAS_REMARC_ACUMULADO'), 2))
    dimensao = dimensao.withColumn('PA', round(col('PA'), 2))
    dimensao = dimensao.withColumn('PA_ACUMULADO', round(col('PA_ACUMULADO'), 2))
    
    dimensao = dimensao.withColumn('MES' , lpad('MES', 2, '0'))
    dimensao = dimensao.withColumn('DT_REFERENCIA', concat(col('ANO'), lit('-'), col('MES')))

    #ATIVO_MARCA
    dimensao = dimensao.withColumn('ATIVO_MARCA' , when(col('QT_TICKET_ACUMULADO') > 0, True).otherwise(False))
    
    #ATIVO_REDE
    ativo_rede = dimensao.select('DT_REFERENCIA', 'CPF', 'QT_TICKET_ACUMULADO')
    ativo_rede = ativo_rede.groupBy('DT_REFERENCIA', 'CPF').agg(sum('QT_TICKET_ACUMULADO').alias('SOMA'))
    ativo_rede = ativo_rede.withColumn('ATIVO_REDE' , when(col('SOMA') > 0, True).otherwise(False)).drop('SOMA')
    dimensao = dimensao.join(ativo_rede, on= ['DT_REFERENCIA', 'CPF'], how = 'inner')
    
    #FLAG_UNICA_COMPRA
    dimensao = dimensao.withColumn('FLAG_UNICA_COMPRA' , when(col('QT_TICKET_ACUMULADO') == 1, True).otherwise(False))
    
    
    #FLAG_PROMO
    dimensao = dimensao.withColumn('FLAG_PROMO', when(col('PER_PECAS_REMARC_ACUMULADO') > 0.3334, True).otherwise(False))
    
    #FLAG_FRANQUIA / ECOM / LOJA
    dimensao = dimensao.withColumn('FLAG_FRANQUIA', when(col('QT_TICKET_FRANQUIA_ACUMULADO') >= 1, True).otherwise(False))
    dimensao = dimensao.withColumn('FLAG_ECOM', when(col('QT_TICKET_ON_ACUMULADO') >= 1, True).otherwise(False))
    dimensao = dimensao.withColumn('FLAG_LOJA_PROPRIA', when(col('QT_TICKET_OFF_ACUMULADO') >= 1, True).otherwise(False))
    
    # Lógica para exclusivo própria, franquia ou compartilhado PERFIL_CLIENTE
    dimensao = dimensao.withColumn(
        "PERFIL_CLIENTE", F.when((F.col("flag_franquia") == True) & (F.col("flag_ecom") == False) & (F.col("flag_loja_propria") == False),"EXCLUSIVO_FRANQUIA")
        .when(((F.col("flag_franquia") == True)&((F.col("flag_ecom") == True)|(F.col("flag_loja_propria") == True))), "COMPARTILHADO")
        .when((F.col("flag_franquia") == False) &((F.col("flag_ecom") == True) |(F.col("flag_loja_propria") == True)),"EXCLUSIVO_PROPRIA").otherwise("NAO_CLASSIFICADO")
    )
    
    #RECORRENCIA
    recorrencia = purchase.filter(col('RECORRENCIA').isin(['Novo', 'Recuperado']))
    cd_partition = Window.partitionBy(col('CPF'), col('MARCA'), col('ANO'), col('MES')).orderBy(col('DATA_COMPRA').desc())
    recorrencia = recorrencia.withColumn('ORDER', row_number().over(cd_partition))
    recorrencia = recorrencia.filter(col('ORDER') == 1)
    recorrencia = recorrencia.withColumn('MES' , lpad('MES', 2, '0'))
    recorrencia = recorrencia.withColumn('DT_REFERENCIA', concat(col('ANO'), lit('-'), col('MES'))).drop('ORDER', 'ANO', 'MES', 'DATA_COMPRA')
    dimensao = dimensao.join(recorrencia, on =  ['DT_REFERENCIA', 'CPF', 'MARCA'], how = 'left')
    dimensao = dimensao.withColumn('RECORRENCIA' , when(col('QT_TICKET_ACUMULADO')<= 0.00, 'Inativo').when(col('RECORRENCIA').isNull(), 'Recorrente').otherwise(col('RECORRENCIA')))
    
    
    
    #DT_ULT_COMRA
    cd_partition = Window.partitionBy(col('CPF'), col('MARCA'), col('ANO'), col('MES')).orderBy(col('DATA_COMPRA').desc())
    ult_compra = purchase.withColumn('ORDER', row_number().over(cd_partition))
    ult_compra = ult_compra.filter(col('ORDER') == 1)
    ult_compra = ult_compra.withColumn('MES' , lpad('MES', 2, '0'))
    ult_compra = ult_compra.withColumn('DT_REFERENCIA', concat(col('ANO'), lit('-'), col('MES'))).drop('ORDER', 'ANO', 'MES', 'RECORRENCIA')
    ult_compra = ult_compra.withColumnRenamed('DATA_COMPRA', 'DT_ULT_COMPRA')
    dimensao = dimensao.join(ult_compra, on =  ['DT_REFERENCIA', 'CPF', 'MARCA'], how = 'left')
    dimensao = dimensao.withColumn('DT_ULT_COMPRA', last(col('DT_ULT_COMPRA'), True).over(Window.partitionBy("CPF","MARCA").orderBy('DT_REFERENCIA')))
    
    
    #CANAL_PREF
    # Alinhado com Milena em 07/07/2025: Não diferenciar mais os híbridos, vai todo mundo cair em híbrido agora!
    dimensao = dimensao.withColumn('CANAL_PREF', when(col('QT_TICKET_ACUMULADO') == 0.00 , 'Sem Canal')\
                                                .when(col('QT_TICKET_ON_ACUMULADO') == col('QT_TICKET_ACUMULADO'), 'Exclusivo On')\
                                                .when((col('QT_TICKET_OFF_ACUMULADO') + col('QT_TICKET_FRANQUIA_ACUMULADO')) == col('QT_TICKET_ACUMULADO'), 'Exclusivo Off')\
                                                .when((col('QT_TICKET_ON_ACUMULADO') == (col('QT_TICKET_OFF_ACUMULADO')+col('QT_TICKET_FRANQUIA_ACUMULADO')) ) & (col('VLF_ON_ACUMULADO') > (col('VLF_OFF_ACUMULADO')+col('VLF_FRANQUIA_ACUMULADO'))), 'Hibrido')\
                                                .when((col('QT_TICKET_ON_ACUMULADO') == (col('QT_TICKET_OFF_ACUMULADO') + col('QT_TICKET_FRANQUIA_ACUMULADO'))  ) & (col('VLF_ON_ACUMULADO') < (col('VLF_OFF_ACUMULADO')+col('VLF_FRANQUIA_ACUMULADO'))), 'Hibrido')\
                                                .when(col('QT_TICKET_ON_ACUMULADO') > (col('QT_TICKET_OFF_ACUMULADO') + col('QT_TICKET_FRANQUIA_ACUMULADO')) , 'Hibrido')\
                                                .when(col('QT_TICKET_ON_ACUMULADO') < (col('QT_TICKET_OFF_ACUMULADO') + col('QT_TICKET_FRANQUIA_ACUMULADO')) , 'Hibrido')\
                                                .when(col('VLF_ON_ACUMULADO') > (col('VLF_OFF_ACUMULADO')+col('VLF_FRANQUIA_ACUMULADO')), 'Hibrido')\
                                                .when(col('VLF_ON_ACUMULADO') < (col('VLF_OFF_ACUMULADO')+col('VLF_FRANQUIA_ACUMULADO')), 'Hibrido')\
                                                .otherwise('Hibrido'))
    
    
    #select dos campos
    #adicionei campos aqui no select
    dimensao = dimensao.select('DT_REFERENCIA', 'CPF', 'MARCA', 'ATIVO_REDE', 'ATIVO_MARCA', 'FLAG_UNICA_COMPRA', 'FLAG_PROMO', 'CANAL_PREF', 'RECORRENCIA', 'DT_ULT_COMPRA', 'VLF', 'VLF_ACUMULADO', 'VLF_OFF', 'VLF_OFF_ACUMULADO', 'VLF_ON', 'VLF_ON_ACUMULADO','VLF_FP', 'VLF_FP_ACUMULADO', 'VLF_MD', 'VLF_MD_ACUMULADO', 'VLF_EC', 'VLF_EC_ACUMULADO', 'QLF', 'QLF_ACUMULADO', 'QLF_FP', 'QLF_FP_ACUMULADO', 'QLF_MD', 'QLF_MD_ACUMULADO', 'QLF_EC', 'QLF_EC_ACUMULADO', 'LUCRO', 'LUCRO_ACUMULADO', 'MARGEM', 'MARGEM_ACUMULADO', 'QT_TICKET', 'QT_TICKET_ACUMULADO', 'QT_TICKET_OFF', 'QT_TICKET_OFF_ACUMULADO', 'QT_TICKET_ON', 'QT_TICKET_ON_ACUMULADO',  'TICKET_MEDIO', 'TICKET_MEDIO_ACUMULADO', 'PRECO_MEDIO', 'PRECO_MEDIO_ACUMULADO', 'FREQUENCIA', 'FREQUENCIA_ACUMULADO', 'PER_PECAS_REMARC', 'PER_PECAS_REMARC_ACUMULADO', 'PA', 'PA_ACUMULADO','PART_EVENTOS','PART_EVENTOS_ACUMULADO','QTD_TLMKT_ACUMULADO','QTD_SMS_ACUMULADO','QTD_EMAIL_ACUMULADO','FLAG_FRANQUIA','FLAG_ECOM', 'FLAG_LOJA_PROPRIA',"PERFIL_CLIENTE")
    
    
    
    # Add column Data_Carga
    data_carga = datetime.now(timezone("Brazil/East"))
    dimensao = dimensao.withColumn("DATA_CARGA", lit(str(data_carga)[:19])).distinct()
    dimensao = dimensao.withColumn('DATA_CARGA', dimensao['DATA_CARGA'].cast(TimestampType()))

    return dimensao
