-- USO: pig -Dpig.additional.jars=utils/elephantbird/*.jar

-- Twitter Elephant Bird
REGISTER utils/elephantbird/*.jar

-- Cargar los datos desde el archivo JSON
tweets = LOAD 'twitter-news/us_news_tweets.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

-- Cargar las palabras clave pro-vacuna
pro_vax_keywords = LOAD 'problemas/tweets_vax/keywords/pro_vax_keywords.txt' USING PigStorage('\n') AS (keyword:chararray);

-- Cargar las palabras clave anti-vacuna
anti_vax_keywords = LOAD 'problemas/tweets_vax/keywords/anti_vax_keywords.txt' USING PigStorage('\n') AS (keyword:chararray);

-- Extraer los campos necesarios
selected_tweets = FOREACH tweets GENERATE
  (chararray) $0#'content' AS content,
  (chararray) $0#'user'#'username' AS username,
  (int) $0#'likeCount' AS likeCount,
  (int) $0#'retweetCount' AS retweetCount;

-- Clasificar tweets como pro-vacuna
cross_pro = CROSS pro_vax_keywords, selected_tweets;
pro_vax_tweets = FILTER cross_pro by (selected_tweets::content MATCHES CONCAT('(?i).*',pro_vax_keywords::keyword,'.*'));

-- Clasificar tweets como anti-vacuna
cross_anti = CROSS anti_vax_keywords, selected_tweets;
anti_vax_tweets = FILTER cross_anti BY (selected_tweets::content MATCHES CONCAT('(?i).*',anti_vax_keywords::keyword,'.*'));

DUMP pro_vax_keywords;
DUMP pro_vax_tweets;
DUMP anti_vax_keywords;
DUMP anti_vax_tweets;

-- Almacenar los resultados
STORE pro_vax_tweets INTO 'problemas/tweets_vax/resultados_tweets_pro' USING PigStorage(',');
STORE anti_vax_tweets INTO 'problemas/tweets_vax/resultados_tweets_anti' USING PigStorage(',')
