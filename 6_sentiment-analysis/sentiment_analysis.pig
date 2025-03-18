REGISTER utils/elephantbird/*.jar

-- Cargar datos desde el archivo de tweets
load_tweets = LOAD 'file:///home/alejandro/Documents/USB/pregrado/big-data/data/us_news_tweets.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

-- Extraer los campos necesarios: ID y texto del tweet
extract_details = FOREACH load_tweets GENERATE 
    (chararray) $0#'id' AS id, 
    (chararray) $0#'content' AS content;

small_extract_details = LIMIT extract_details 10;

-- Dividir el texto del tweet en palabras (tokens)
tokens = FOREACH small_extract_details GENERATE 
    id, 
    content, 
    FLATTEN(TOKENIZE(content)) AS word;

-- Cargar el léxico AFINN con las palabras y sus calificaciones
dictionary = LOAD 'AFINN/AFINN-111.txt' USING PigStorage('\t') AS (word:chararray, rating:int);

-- small_tokens = LIMIT tokens 1000;

-- Unir tokens de los tweets con el léxico para asignar calificaciones
word_rating = JOIN tokens BY word LEFT OUTER, dictionary BY word USING 'replicated';

-- Extraer ID del tweet, el texto y la calificación de las palabras
rating = FOREACH word_rating GENERATE 
    tokens::id AS id, 
    tokens::content AS content, 
    dictionary::rating AS rate;

-- Agrupar las calificaciones por cada tweet
word_group = GROUP rating BY (id, content);

-- Calcular el promedio de calificaciones por tweet
avg_rate = FOREACH word_group GENERATE 
    group.id AS id, 
    group.content AS content, 
    AVG(rating.rate) AS tweet_rating;

-- Filtrar los tweets en positivos y negativos
positive_tweets = FILTER avg_rate BY tweet_rating >= 0;
negative_tweets = FILTER avg_rate BY tweet_rating < 0;

STORE positive_tweets INTO 'output/positive_tweets' USING PigStorage(',');
STORE negative_tweets INTO 'output/negative_tweets' USING PigStorage(',');

