# sqx

## Installation

```sh
$ go install github.com/tys-muta/go-sqx.git
```

## SQLite

#### Generate Database from Git repository

##### Command

```sh
$ go-sqx sqlite gen foo.db github.com/tys-muta/go-sqx.git --refs refs/tags/v0.x.x
```

##### Configure ( .sqx.toml )

```toml
# ソフトウェア共通の設定

[sqlite.gen]
repo = "https://github.com/tys-muta/go-sqx.git" # 対象のリポジトリ ( ここで指定する場合はコマンド引数のリポジトリ指定は不要 )
refs = "refs/tags/v0.x.x" # 対象の参照情報

[sqlite.gen.basicAuth]
username = "xxx" # Git のユーザー名
password = "ghp_xxx" # Git のパスワード ( GitHub ならパーソナルアクセストークン )

[sqlite.gen.head]
ext = ".xlsx" # 対象となる表ファイルの拡張子
path = "example/xlsx" # 取り込みの起点となるリポジトリルートからのパス
columnNameRow = 3 # カラム名が定義されている行数
columnTypeRow = 2 # カラムの型が定義されている行数 ( string, datetime, int, float )

[sqlite.gen.body]
ext = ".xlsx" # 対象となる表ファイルの拡張子
path = "example/xlsx" # 取り込みの起点となるリポジトリルートからのパス
startRow = 4 # 取り込みを開始する行数

[sqlite.gen.xlsx]
sheet = "データ" # 取り込み対象のシート名


# 以降はテーブルごとの定義

[sqlite.gen.table."/standard"] 
primaryKey = ["id"]
uniqueKeys = [
  ["floatColumn", "datetimeColumn"],
]
indexKeys = [
  ["stringColumn"],
  ["intColumn", "floatColumn"],
]

[sqlite.gen.table."/shard/int"]
primaryKey = ["typeId", "id"]
# shardXXX は、ファイルを分割している場合の設定
shardColumnName = "typeId" # ファイルに含まれるデータを格納する SQLite テーブルは、ここで指定した名称のカラムを持つ
shardColumnType = "int" # ファイル名はこの型として格納される

[sqlite.gen.table."/shard/string"]
primaryKey = ["type", "id"]
shardColumnName = "type"
shardColumnType = "string"
```

