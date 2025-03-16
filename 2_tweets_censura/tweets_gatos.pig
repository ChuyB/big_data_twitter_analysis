-- USO: pig -Dpig.additional.jars=utils/elephantbird/*.jar

-- Twitter Elephant Bird
REGISTER utils/elephantbird/*.jar

-- Cargar los datos desde el archivo JSON
tweets = LOAD 'twitter-news/us_news_tweets.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

-- Extraer los campos necesarios
selected_tweets = FOREACH tweets GENERATE
  (chararray) REPLACE($0#'content','\n',' ') AS content,
  (chararray) $0#'user'#'username' AS username,
  (int) $0#'likeCount' AS likeCount,
  (int) $0#'retweetCount' AS retweetCount;

-- Cargar las palabras clave da gatos
cat_keywords = LOAD 'problemas/tweets_gatos/cat_keywords.txt' USING PigStorage('\n') AS (keyword:chararray);

-- Clasificar tweets de temas de gatos
cross_cat = CROSS cat_keywords, selected_tweets;
cat_tweets = FILTER cross_cat BY (selected_tweets::content MATCHES CONCAT('(?i).* ',cat_keywords::keyword,' .*'));
cat_tweets_2 = FOREACH cat_tweets GENERATE selected_tweets::content AS content, selected_tweets::username AS username, selected_tweets::likeCount AS likeCount, selected_tweets::retweetCount AS retweetCount;

-- Eliminar los tweets relacionados a gatos
-- Realizar un LEFT OUTER JOIN
join_result = JOIN selected_tweets BY content LEFT OUTER, cat_tweets_2 BY content;
-- Filtrar los elementos que están en conjuntoA pero no en conjuntoB
diferencia = FILTER join_result BY cat_tweets_2::content IS NULL;
-- Seleccionar sólo las columnas del conjunto A
not_cat_tweets = FOREACH diferencia GENERATE selected_tweets::content, selected_tweets::username, selected_tweets::likeCount AS likeCount, selected_tweets::retweetCount AS retweetCount;

-- Almacenar los tweets que se van a eliminar para mejor informacion
STORE cat_tweets INTO 'problemas/tweets_gatos/resultados_tweets_gatos' USING PigStorage('|');

-- Almacenar los tweets que no estan relacionados a los gatos
STORE not_cat_tweets INTO 'problemas/tweets_gatos/resultados_tweets_no_gatos' USING PigStorage('|');

-- Contamos y guardamos el numero de tweets relacionados a temas de gatos encontrados
grouped_cat = GROUP cat_tweets ALL;
count_tuples_cat = FOREACH grouped_cat GENERATE COUNT(cat_tweets);
STORE count_tuples_cat INTO 'problemas/tweets_gatos/numero_de_tweets_gatos' USING PigStorage('|');