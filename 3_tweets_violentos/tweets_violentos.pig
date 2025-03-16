-- USO: pig -Dpig.additional.jars=utils/elephantbird/*.jar

-- Twitter Elephant Bird
REGISTER utils/elephantbird/*.jar;

-- Cargar los datos desde el archivo JSON
tweets = LOAD 'twitter-news/us_news_tweets.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

-- Extraer los campos necesarios
selected_tweets = FOREACH tweets GENERATE
  (chararray) REPLACE($0#'content','\n',' ') AS content,
  (chararray) $0#'user'#'username' AS username,
  (int) $0#'likeCount' AS likeCount,
  (int) $0#'retweetCount' AS retweetCount,
  (chararray) $0#'place'#'country' AS country,
  (chararray) $0#'place'#'fullName' AS city_state;

-- Clasificar tweets de usa
us_tweets = FILTER selected_tweets by (country == 'United States');

-- Cargar las palabras clave violentas
vio_keywords = LOAD 'problemas/tweets_violentos/vio_keywords.txt' USING PigStorage('\n') AS (keyword:chararray);

-- Clasificar tweets como violentos
cross_vio = CROSS vio_keywords, us_tweets;
vio_tweets = FILTER cross_vio by (us_tweets::content MATCHES CONCAT('(?i).*',vio_keywords::keyword,'.*'));

-- Extrer el estado
tweets_states = FOREACH vio_tweets GENERATE content AS content, username AS username, likeCount AS likeCount, retweetCount AS retweetCount, REGEX_EXTRACT(city_state, '[^,]+,\\s*(.*)', 1) AS state;

-- Contamos los tweets por grupo
count_tweets = FOREACH (GROUP tweets_states BY state) GENERATE group AS state, COUNT(tweets_states) AS tweet_count, FLATTEN(tweets_states);

-- Agrupa por estado
tweets_final = FOREACH (GROUP count_tweets BY (state,tweet_count)) {
        tweets = FOREACH count_tweets GENERATE content,username,likeCount,retweetCount;
        GENERATE FLATTEN(group), tweets;
}

-- Ordenamos los tweets por el numero de tweets por estado
order_tweets = ORDER tweets_final BY tweet_count DESC;

-- Almacenar los resultados
STORE order_tweets INTO 'problemas/tweets_violentos/resultados_tweets_violentos_ordenados' USING PigStorage('|');