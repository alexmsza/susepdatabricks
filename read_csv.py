def criar_view_temporaria(nome_tabela):
    # Construindo o caminho do arquivo CSV com base no nome da tabela
    caminho_csv = f"/Volumes/workspace/default/susep-data/autoseg2021/raw/{nome_tabela}.csv"
    
    # Leitura, configuração e criação da view temporária em uma ÚNICA instrução encadeada
    (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ';')
        .load(caminho_csv)
        .createOrReplaceTempView(nome_tabela)
    )
    
    # Exibindo as primeiras 10 linhas da view temporária
    print(f"\nTabela: {nome_tabela}")
    spark.sql(f"SELECT * FROM {nome_tabela} LIMIT 10").display()


criar_view_temporaria(nome_tabela="auto_cau")
criar_view_temporaria(nome_tabela="auto_cat")
criar_view_temporaria(nome_tabela="auto_cep")
criar_view_temporaria(nome_tabela="auto_cidade")
criar_view_temporaria(nome_tabela="auto_cob")
criar_view_temporaria(nome_tabela="auto_idade")
criar_view_temporaria(nome_tabela="auto_reg")
criar_view_temporaria(nome_tabela="auto_sexo")
criar_view_temporaria(nome_tabela="auto2_grupo")
criar_view_temporaria(nome_tabela="auto2_vei")
criar_view_temporaria(nome_tabela="PremReg")
criar_view_temporaria(nome_tabela="arq_casco_comp")
criar_view_temporaria(nome_tabela="arq_casco3_comp")
criar_view_temporaria(nome_tabela="arq_casco4_comp")
criar_view_temporaria(nome_tabela="SinReg")
