# openlineage

openlineage とは来歴管理の標準化を目指すための、APIインターフェースとデータ構造の定義である。

## インターフェース

openlineage は次のインターフェースのみ公開し、イベント駆動な作り（リソース指向ではない）となっている。

```
POST api/v1/lineage
```

詳しくは公式サイトを参照とする。

https://openlineage.io/getting-started/

## 勘所

* イベントデータのpublish用APIとイベントデータの構造のみを定義している
  * そのイベントデータがどのようなリソースとして管理されるかは実装次第
  * 状態を復元するにはイベントデータ群を同じ順番で送信するという考えが自然だろう
* openlineage に準拠したツールは参照系のオプションと位置付けるべきだろう
  * ファセットは数多くあり、全てをきちんと埋めることは難しいだろう
    * 何度か再インデックスする必要があるだろう
  * 信頼できる永続層としては、主体となるシステムが役割を担うべきだろう
    * 二重管理にはなる
    * オプションの位置づけ
    * 主体となるシステムのデータを openlineage に変換する層が必要
  * GUI ツールの開発を簡略化することができる
    * openlineage に適用させる変換層が必要で二度手間になる(openlineage を準拠した構造を持てるのが理想）
    * どのみち優先度は主体となるデータベースへの永続化の方法


# MARQUEZ

MARQUEZ は、openlineage に準拠したリファレンス実装の１つである。

`POST api/v1/lineage` に加えて、各リソースの Delete などを実装している。

## 永続化層

### lineage_events

イベントデータをそのまま保存する。

```
Column		Type		Size		Nulls	Auto	Default	Children	Parents	Comments
event_time	timestamptz	35,6				null			
event		jsonb		2147483647			null			
event_type	text		2147483647	√		null			
job_name	text		2147483647	√		null			
job_namespace	text		2147483647	√		null			
producer	text		2147483647	√		null			
run_uuid	uuid		2147483647	√		null			
created_at	timestamptz	35,6		√		((now() AT TIME ZONE 'UTC'::text))::timestamp with time zone			
_event_type	varchar		64		√		'RUN_EVENT'::character varying			
```

### 重要になりそうなテーブル

* Jobs
  * 内部的に uuid が割り当てられているが、それはリクエスト時に渡せない
  * name はユニークではない
* Runs
  * uuid が必要
* Datasets
  * 内部的に uuid が割り当てられているが、それはリクエスト時に渡せない
    * name はユニークではない
    * ddcs は uuid でレコードを特定していなかったはず
    * model に関しては model_id が uuid 。モデルストアで名前空間が分けられる？
      * s3 の命名規則を参考がいいはず(エンドポイントは命名規則に含まれないようだ)。namespace => バケット名 path => s3://{バケット名}/{パス}
        * endpoint は公開しない場合はあるだろう
