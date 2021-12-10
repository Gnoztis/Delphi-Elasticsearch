# Delphi-Elasticsearch
Delphi for Elasticsearch

```
var
  JsonObject: TJSONObject;
  SubJsonObject: TJSONObject;
  ElasticClient : TElasticClient;
begin
	ElasticClient := TElasticClient.Create;
	ElasticClient.host := '<ip>';
	ElasticClient.port := 9200;

	try
         ElasticClient.connect;
        except
         on E : Exception do
                    ShowMessage( E.Message );
        end;

	if ElasticClient.connected then
		begin
		  ElasticClient.CreateIndex('test');
		  
		  JsonObject:=TJSONObject.Create;
		  SubJsonObject:=TJSONObject.Create;

		  try
			JsonObject.AddPair('@timestamp', estimestamp );
			JsonObject.AddPair('uuid', uuid);

			SubJsonObject.AddPair('current',  '10'  );
			SubJsonObject.AddPair('avg',  '7'  );
	
			JsonObject.AddPair('sub',SubJsonObject);

			ElasticClient._doc(JsonObject.ToString,'test');
		  finally
			JsonObject.Free;
		  end;

		end;
	
	ElasticClient.Destroy;
end;
```