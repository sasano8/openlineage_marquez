# Opnelineage

Openlineage は来歴管理の標準化を目指す仕様です。


## 仕様の概要

インターフェースは、イベントデータを登録するAPIと、イベントデータから成ります。

```
POST api/v1/lineage
```

詳しくは公式サイトを参照してください。

https://openlineage.io/getting-started/


## MARQUEZ を試してみる

Openlineage のリファレンス実装として MARQUEZ という WEB アプリがあります。

```
docker-compose.yml up -d
```

仮想環境を作って、ライブラリをインストールします。

```
python3 -m venv .venv
pip install -r requirements.txt
.venv/source/bin/activate
```

データを登録します。

```
pytest -xvx .
```

`http://localhost:3000` で結果を確認します。



## MARQUEZ を解析する

テーブル構造など解析する手順を示す。

接続情報やその他をオプションを任意に指定し、schemaspy を起動します。
解析結果は `schema` に出力されます。

```
docker run \
  -v "$PWD/schema:/output" \
  --net="marquez_default" \
  schemaspy/schemaspy:snapshot \
  -t pgsql \
  -host marquez-db \
  -port 5432 \
  -db marquez \
  -u postgres \
  -p password
```

解析したhtmlファイルをhttpサーバーで公開します。

```
python -m http.server -d schema 8000
```


### トラブルシューティング

#### docker コンテナ上のDB に接続できない

* `docker ps` の `NAMES` の値をホストとして指定していますか？
* 同じネットワークに属していますか？
  * `docker inspect <ID>` で確認したネットワークを`--net` で指定してください

