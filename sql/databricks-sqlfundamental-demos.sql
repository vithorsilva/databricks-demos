-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks - SQL Demo
-- MAGIC * **Autor:** Vithor da Silva e Silva
-- MAGIC * **Contato:** vithor@datasource.expert / https://github.com/vithorsilva
-- MAGIC * **Data:** 2024-10-03
-- MAGIC * **Objetivo:** Demonstrar comandos essenciais da linguagem SQL aplicado no Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Sobre: nyctaxi**
-- MAGIC Este é o banco de demonstração já disponível em todo workspace Databricks no catalogo samples.

-- COMMAND ----------

use samples.tpch

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SELECT - Fundamental

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Básico
-- MAGIC A clausula mais básica é sem dúvidas a SELECT * FROM OBJETO, onde todas colunas e linhas serão exibidas do objeto (tabela ou view) que você solicitar.

-- COMMAND ----------

select * from orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Imaginando que o objeto selecionado pode conter milhões de registros, recomenda-se sempre a inclusão de um limit para trabalhar assim com uma amostra de dados.

-- COMMAND ----------

select * from orders limit 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Filtrando
-- MAGIC Esta consulta SQL recupera todas as colunas (*) da tabela orders, que é referenciada como o. Ela filtra os resultados para incluir apenas as linhas onde a coluna o_orderstatus tem o valor 'F'. 
-- MAGIC O alias o é usado para simplificar a referência à tabela orders dentro da consulta.

-- COMMAND ----------

SELECT * 
FROM orders as o
where o.o_orderstatus = 'F'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Colunas
-- MAGIC
-- MAGIC Esta consulta SQL recupera detalhes específicos da tabela orders para pedidos que têm o status 'F'. Aqui está uma explicação de cada parte da consulta:
-- MAGIC
-- MAGIC * SELECT: Especifica as colunas a serem recuperadas que são:
-- MAGIC   * o.o_orderkey
-- MAGIC   * o.o_custkey
-- MAGIC   * o.o_totalprice
-- MAGIC   * year(o.o_orderdate) as year: Extrai o ano da data do pedido e o rotula como year.
-- MAGIC   * o.o_orderpriority as priority: Recupera a prioridade do pedido e a rotula como priority.
-- MAGIC * FROM orders as o: Especifica a tabela orders como a fonte dos dados e atribui a ela o alias o.
-- MAGIC * WHERE o.o_orderstatus = 'F': Filtra os resultados para incluir apenas os pedidos onde o status do pedido é 'F'.
-- MAGIC
-- MAGIC Em resumo, a consulta busca a chave do pedido, a chave do cliente, o preço total, o ano da data do pedido e a prioridade do pedido para todos os pedidos com status 'F'.
-- MAGIC

-- COMMAND ----------

SELECT 
  o.o_orderkey, 
  o.o_custkey, 
  o.o_totalprice, 
  year(o.o_orderdate) as year, 
  o.o_orderpriority as priority
FROM orders as o
where o.o_orderstatus = 'F'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Junções
-- MAGIC Este código SQL realiza uma consulta para selecionar informações específicas de pedidos concluídos, combinando dados das tabelas orders e customer.
-- MAGIC
-- MAGIC Algumas colunas são selecionadas, mas observe que agora uma nova coluna foi adicionada e de uma tabela que está complementando o resultado, indicando qual o segmento o cliente que comprou pertence.
-- MAGIC
-- MAGIC ```sql
-- MAGIC INNER JOIN customer as c ON c.c_custkey = o.o_custkey
-- MAGIC ```
-- MAGIC Combina as tabelas orders e customer onde a chave do cliente (c_custkey) na tabela customer corresponde à chave do cliente (o_custkey) na tabela orders.
-- MAGIC

-- COMMAND ----------

SELECT 
  o.o_orderkey, 
  c.c_mktsegment as segment,
  o.o_totalprice, 
  year(o.o_orderdate) as year, 
  o.o_orderpriority as priority
FROM orders as o
INNER JOIN customer as c ON c.c_custkey = o.o_custkey
where o.o_orderstatus = 'F'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Agrupando dados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemplo 1
-- MAGIC Somente uma coluna sendo agrupada.
-- MAGIC
-- MAGIC 1. **SELECT**: Esta cláusula está selecionando três colunas:
-- MAGIC     * c.c_mktsegment como segment: O segmento de mercado do cliente.
-- MAGIC     * SUM(o.o_totalprice) como total_price: A soma dos preços totais dos pedidos.
-- MAGIC     * count(*) como qty_orders: A contagem total de pedidos.
-- MAGIC
-- MAGIC 2. **FROM** orders as o: Especifica a tabela orders com o alias o.
-- MAGIC
-- MAGIC 3. **INNER JOIN** customer as c ON c.c_custkey = o.o_custkey: Realiza uma junção interna entre a tabela orders (pedidos) e a tabela customer (clientes) onde a chave do cliente (c_custkey) na tabela customer corresponde à chave do cliente (o_custkey) na tabela orders.
-- MAGIC
-- MAGIC 4. **WHERE** o.o_orderstatus = 'F': Filtra os resultados para incluir apenas os pedidos cujo status (o_orderstatus) é 'F' (finalizado).
-- MAGIC
-- MAGIC 5. **GROUP BY** c.c_mktsegment: Agrupa os resultados pelo segmento de mercado do cliente (c_mktsegment).
-- MAGIC
-- MAGIC Em resumo, este código SQL está calculando o total de preços e a quantidade de pedidos finalizados, agrupados por segmento de mercado dos clientes.

-- COMMAND ----------

SELECT 
  c.c_mktsegment as segment,
  SUM(o.o_totalprice) total_price, 
  count(*) qty_orders
FROM orders as o
INNER JOIN customer as c ON c.c_custkey = o.o_custkey
where o.o_orderstatus = 'F'
group by c.c_mktsegment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemplo 2
-- MAGIC Mais de uma coluna sendo agrupada, semelhante ao caso anterior, porém adicionando uma coluna que é o ano da venda (extraida por função de uma coluna de data/hora).

-- COMMAND ----------

SELECT 
  c.c_mktsegment as segment,
  year(o.o_orderdate) as year,
  SUM(o.o_totalprice) total_price, 
  count(*) qty_orders
FROM orders as o
INNER JOIN customer as c ON c.c_custkey = o.o_custkey
where o.o_orderstatus = 'F'
group by c.c_mktsegment, year(o.o_orderdate)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SELECT - Intermediário

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. PIVOT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemplo 1
-- MAGIC O código SQL realiza uma consulta para pivotar os dados de pedidos finalizados, agrupando-os por segmento de mercado e ano. 
-- MAGIC
-- MAGIC A consulta interna agrega os dados e a consulta externa realiza a operação de pivot.

-- COMMAND ----------

SELECT d.segment, d.`1992`, d.`1993`, d.`1994`, d.`1995`
FROM (
  SELECT *
  FROM (
    SELECT 
      c.c_mktsegment as segment,
      year(o.o_orderdate) as year_order,
      SUM(o.o_totalprice) total_price
    FROM orders as o
    INNER JOIN customer as c ON c.c_custkey = o.o_custkey
    WHERE o.o_orderstatus = 'F'
    GROUP BY c.c_mktsegment, year(o.o_orderdate)
  ) 
  PIVOT (
    sum(total_price) for (year_order) IN ('1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999')
  ) 
) d

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemplo 2
-- MAGIC O código SQL está agregando os preços totais dos pedidos finalizados por segmento de mercado e região, e depois transforma esses dados para que os totais anuais apareçam como colunas separadas para os anos de 1992 a 1995.
-- MAGIC
-- MAGIC * **Subconsulta Interna**:
-- MAGIC   * A subconsulta interna seleciona dados das tabelas orders, customer, nation e region.
-- MAGIC   * As tabelas são unidas (INNER JOIN) com base em chaves estrangeiras.
-- MAGIC   * Filtra os pedidos (orders) que têm o status 'F'.
-- MAGIC   * Agrupa os resultados por segmento de mercado do cliente (c.c_mktsegment), nome da região (r.r_name) e ano do pedido (year(o.o_orderdate)).
-- MAGIC   * Calcula a soma dos preços totais dos pedidos (SUM(o.o_totalprice)) para cada grupo.
-- MAGIC
-- MAGIC * **PIVOT**:
-- MAGIC   * A subconsulta intermediária aplica uma operação de pivot para transformar linhas em colunas.
-- MAGIC   * A operação de pivot agrupa os dados por year_order e calcula a soma dos preços totais (sum(total_price)) para os anos de 1990 a 1999.

-- COMMAND ----------

SELECT d.region, d.segment,  d.`1992`, d.`1993`, d.`1994`, d.`1995`
FROM (
  SELECT *
  FROM (
    SELECT 
      c.c_mktsegment as segment, 
      r.r_name as region,
      year(o.o_orderdate) as year_order,
      SUM(o.o_totalprice) total_price
    FROM orders as o
    INNER JOIN customer as c ON c.c_custkey = o.o_custkey
    INNER JOIN nation as n ON n.n_nationkey = c.c_nationkey
    INNER JOIN region as r ON r.r_regionkey = n.n_regionkey
    WHERE o.o_orderstatus = 'F'
    GROUP BY c.c_mktsegment, r.r_name, year(o.o_orderdate)
  ) 
  PIVOT (
    sum(total_price) for (year_order) IN ('1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999')
  ) 
) d

-- COMMAND ----------

select * from region

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Funções

-- COMMAND ----------

select * from lineitem as li
limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemplo Básico
-- MAGIC Funções de agregação apoiam na realização de cálculos ou operações em cima de colunas.
-- MAGIC
-- MAGIC Este código SQL realiza uma consulta na tabela lineitem (li) e retorna as seguintes informações:
-- MAGIC * qtd_rows: Contagem total de linhas.
-- MAGIC * total_orders: Contagem de pedidos distintos (l_orderkey).
-- MAGIC * qtd_distinct_produtos: Contagem de produtos distintos (l_partkey).
-- MAGIC * total_revenue: Soma dos preços estendidos (l_extendedprice).
-- MAGIC * total_profit: Soma dos preços estendidos ajustados pelo desconto (l_extendedprice * (1 - l_discount)).
-- MAGIC * min_quantity: Quantidade mínima (l_quantity).
-- MAGIC * avg_quantity: Quantidade média (l_quantity).
-- MAGIC * max_quantity: Quantidade máxima (l_quantity).

-- COMMAND ----------

select 
  COUNT(*) qtd_rows,
  COUNT(DISTINCT li.l_orderkey) as total_orders,
  COUNT(DISTINCT li.l_partkey) qtd_distinct_produtos,
  SUM(li.l_extendedprice) as total_revenue,
  SUM(li.l_extendedprice * (1 - li.l_discount)) as total_profit,
  MIN(li.l_quantity) as min_quantity,
  AVG(li.l_quantity) as avg_quantity,
  MAX(li.l_quantity) as max_quantity  
from lineitem as li

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Agregações por Coluna
-- MAGIC O caso anterior, exibiam indicadores calculados para toda a massa de dados, mas e se fosse necessário exibir os dados pela data da venda?
-- MAGIC Ou seja, quantos registros, produtos distintos dentre outros, foram comercializados dia a dia?

-- COMMAND ----------

select 
  o.o_orderdate,
  COUNT(*) qtd_rows,
  COUNT(DISTINCT li.l_orderkey) as total_orders,
  COUNT(DISTINCT li.l_partkey) qtd_distinct_produtos,
  SUM(li.l_extendedprice) as total_revenue,
  MAX(li.l_extendedprice) as max_revenue,
  MIN(li.l_extendedprice) as min_revenue,
  AVG(li.l_extendedprice) as avg_revenue,
  SUM(li.l_extendedprice * (1 - li.l_discount)) as total_profit,
  AVG(li.l_quantity) as avg_quantity
from lineitem as li
INNER JOIN orders as o ON o.o_orderkey = li.l_orderkey
group by o.o_orderdate
order by o_orderdate ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Agregações por Coluna II
-- MAGIC E se fosse necessário ver os indicadores apenas por ano e mês, baseado na coluna orderdate?
-- MAGIC Faremos o uso de funções escalares como MONTH() e YEAR() para extrair informações originalmente de uma coluna.

-- COMMAND ----------

select 
  year(o.o_orderdate) as year,
  month(o.o_orderdate) as month,
  COUNT(DISTINCT li.l_orderkey) as total_orders,
  COUNT(DISTINCT li.l_partkey) qtd_distinct_produtos,
  SUM(li.l_extendedprice) as total_revenue
from lineitem as li
INNER JOIN orders as o ON o.o_orderkey = li.l_orderkey
group by year(o.o_orderdate), month(o.o_orderdate)
order by year, month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. GROUP BY avançado

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GROUPING SETS - Exemplo 1
-- MAGIC O uso de GROUPING SETS no seu código SQL oferece várias vantagens:
-- MAGIC
-- MAGIC **Flexibilidade na Agregação:** GROUPING SETS permite definir múltiplos níveis de agregação em uma única consulta. No seu caso, você está agregando por ano, por mês, por ano e mês combinados, e um total geral. Isso elimina a necessidade de escrever múltiplas consultas separadas para cada nível de agregação.
-- MAGIC
-- MAGIC **Redução de Código:** Em vez de escrever várias consultas com diferentes cláusulas GROUP BY, você pode consolidar tudo em uma única consulta. Isso torna o código mais limpo e fácil de manter.
-- MAGIC
-- MAGIC **Melhor Desempenho:** Usar GROUPING SETS pode ser mais eficiente do que executar várias consultas separadas, pois o banco de dados pode otimizar a execução interna para calcular todos os agrupamentos em uma única varredura dos dados.
-- MAGIC
-- MAGIC **Facilidade de Interpretação:** A função GROUPING_ID ajuda a identificar o contexto de cada linha no resultado, indicando quais colunas foram usadas para agrupar os dados. Isso facilita a interpretação dos resultados agregados.
-- MAGIC
-- MAGIC **Versatilidade:** GROUPING SETS é uma funcionalidade poderosa que pode ser combinada com outras funções de agregação e cláusulas SQL, oferecendo uma grande versatilidade na análise de dados.
-- MAGIC
-- MAGIC **Redução de Erros:** Consolidar múltiplos agrupamentos em uma única consulta reduz a probabilidade de erros que podem ocorrer ao manter várias consultas separadas.

-- COMMAND ----------

select 
  GROUPING_ID(year(o.o_orderdate), month(o.o_orderdate)) as Contexto,
  year(o.o_orderdate) as year,
  month(o.o_orderdate) as month,
  COUNT(DISTINCT li.l_orderkey) as total_orders,
  COUNT(DISTINCT li.l_partkey) qtd_distinct_produtos,
  SUM(li.l_extendedprice) as total_revenue
from lineitem as li
INNER JOIN orders as o ON o.o_orderkey = li.l_orderkey
group by GROUPING SETS (
    (year(o.o_orderdate)),
    (month(o.o_orderdate)),
    (year(o.o_orderdate), month(o.o_orderdate)),
    ()
    )
order by Contexto, year, month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GROUPING SETS - Exemplo 2

-- COMMAND ----------

select 
  GROUPING_ID(year(o.o_orderdate), pa.p_type, month(o.o_orderdate)) as Contexto,
  pa.p_type as producttype,
  year(o.o_orderdate) as year,
  month(o.o_orderdate) as month,
  COUNT(DISTINCT li.l_orderkey) as total_orders,
  COUNT(DISTINCT li.l_partkey) qtd_distinct_produtos,
  SUM(li.l_extendedprice) as total_revenue
from lineitem as li
INNER JOIN orders as o ON o.o_orderkey = li.l_orderkey
INNER JOIN part as pa ON pa.p_partkey = li.l_partkey
group by GROUPING SETS (
    (year(o.o_orderdate)),
    (year(o.o_orderdate), pa.p_type),
    (month(o.o_orderdate)),
    (year(o.o_orderdate), month(o.o_orderdate)),
    ()
    )
order by Contexto, year, month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GROUP BY CUBE

-- COMMAND ----------

select 
  GROUPING_ID(year(o.o_orderdate), month(o.o_orderdate)) as Contexto,
  year(o.o_orderdate) as year,
  month(o.o_orderdate) as month,
  COUNT(DISTINCT li.l_orderkey) as total_orders,
  COUNT(DISTINCT li.l_partkey) qtd_distinct_produtos,
  SUM(li.l_extendedprice) as total_revenue
from lineitem as li
INNER JOIN orders as o ON o.o_orderkey = li.l_orderkey
group by CUBE (year(o.o_orderdate), (month(o.o_orderdate)))
order by Contexto, year, month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GROUP BY ROLLUP

-- COMMAND ----------

select 
  GROUPING_ID(year(o.o_orderdate), month(o.o_orderdate)) as Contexto,
  year(o.o_orderdate) as year,
  month(o.o_orderdate) as month,
  COUNT(DISTINCT li.l_orderkey) as total_orders,
  COUNT(DISTINCT li.l_partkey) qtd_distinct_produtos,
  SUM(li.l_extendedprice) as total_revenue
from lineitem as li
INNER JOIN orders as o ON o.o_orderkey = li.l_orderkey
group by ROLLUP (year(o.o_orderdate), (month(o.o_orderdate)))
order by Contexto, year, month
