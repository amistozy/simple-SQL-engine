module SimpleSQLEngine.Kata (sqlEngine) where

import Data.Bifunctor (Bifunctor (first))
import Data.Char
import Data.Maybe (fromMaybe)
import Data.Traversable (for)
import Text.ParserCombinators.ReadP

type TableName = String
type ColumnName = String
type ColumnId = String
type Value = String
type Object = [(ColumnName, Value)]
type ResultItem = [(ColumnId, Value)]
type Table = (TableName, [Object])
type DataBase = [Table]
type Comparison = Value -> Value -> Bool
type TestValue = ResultItem -> Maybe Value

data ValueTest = ValueTest TestValue Comparison TestValue

data Query = Query [ColumnId] TableName [(TableName, ValueTest)] (Maybe ValueTest)

string' :: String -> ReadP String
string' "" = pure ""
string' (c : cs) = (:) <$> (char c <++ char (swapCase c)) <*> string' cs
 where
  swapCase c
    | isUpper c = toLower c
    | isLower c = toUpper c
    | otherwise = c

comparisonList :: [(String, Comparison)]
comparisonList =
  [ ("=", (==))
  , (">", (>))
  , ("<", (<))
  , ("<=", (<=))
  , (">=", (>=))
  , ("<>", (/=))
  ]

queryParser :: ReadP Query
queryParser = Query <$> selectP <*> fromP <*> many joinP <*> whereP
 where
  selectP = wsP *> string' "SELECT" *> ws1P *> sepBy1 idP (token $ char ',')
  idP = munch1 $ \c -> isAlphaNum c || c `elem` "._"
  fromP = keyWord "FROM" *> idP
  joinP = (,) <$> (keyWord "JOIN" *> idP) <*> (keyWord "ON" *> valueTestP)
  valueTestP = ValueTest <$> valueP <*> comparisonP <*> valueP
  valueP = (lookup <$> idP) +++ (const . Just <$> constP)
  stringP = many $ satisfy (/= '\'') +++ ('\'' <$ string "''")
  constP = between (char '\'') (char '\'') stringP
  comparisonP = choice $ map (\(s, cmp) -> cmp <$ token (string s)) comparisonList
  whereP = option Nothing $ Just <$> (keyWord "WHERE" *> valueTestP)

  wsP = skipMany $ satisfy isSpace
  ws1P = skipMany1 $ satisfy isSpace
  token = between wsP wsP
  token1 = between ws1P ws1P
  keyWord = token1 . string'

label :: TableName -> [Object] -> [ResultItem]
label tableName = map $ map $ first (\c -> tableName ++ '.' : c)

runTest :: ValueTest -> ResultItem -> Bool
runTest (ValueTest value1 cmp value2) item =
  case (,) <$> value1 item <*> value2 item of
    Nothing -> False
    Just (v1, v2) -> cmp v1 v2

sqlEngine :: DataBase -> String -> [ResultItem]
sqlEngine database = execute
 where
  execute :: String -> [ResultItem]
  execute query =
    case readP_to_S queryParser query of
      [] -> []
      qs -> exec $ fst $ last qs

  getTable :: TableName -> [ResultItem]
  getTable tableName = maybe [] (label tableName) (lookup tableName database)

  reducer :: [ResultItem] -> (TableName, ValueTest) -> [ResultItem]
  reducer table1 (tableName, valueTest) = filter (runTest valueTest) $ (++) <$> table1 <*> table2
   where
    table2 = getTable tableName

  exec :: Query -> [ResultItem]
  exec (Query columnIds tableName joins whereCmp) = selectResult
   where
    initResult = getTable tableName
    joinsResult = foldl reducer initResult joins
    whereResult =
      case whereCmp of
        Nothing -> joinsResult
        Just valueTest -> filter (runTest valueTest) joinsResult
    selectResult =
      fromMaybe [] $
        for whereResult $ \item ->
          for columnIds $ \columnId -> do
            value <- lookup columnId item
            return (columnId, value)