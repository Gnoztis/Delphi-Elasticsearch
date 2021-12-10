unit ElasticSearch;

interface

uses
  System.Net.URLClient, System.Net.HttpClient, System.Net.HttpClientComponent,
  System.Classes, System.SysUtils,
  json, Winapi.Windows;


  type
    tversion = record
       numder: string;
       major : Byte;
    end;

  type
    TElasticClient = class(TObject)
    private
      NetHTTPClient: TNetHTTPClient;
      BaseUrl:String;
      version: tversion;
      parameters: TStringStream;
      HTTPRequest: TNetHTTPRequest;
    public
      host: string;
      port: word;
      Asynchronous: boolean;
      Response: IHTTPResponse;

      constructor Create;
      destructor Destroy; override;

      function connect:boolean;
      function connected:boolean;

      function get_version:string;
      //index
      function indices:IHTTPResponse;
      function CreateIndex(IndexName:String):IHTTPResponse; overload;
      function CreateIndex(IndexName:String; template:string ):IHTTPResponse; overload;
      function DeleteIndex(IndexName:String):IHTTPResponse;
      function _freeze(IndexName:String):IHTTPResponse;
      function _unfreeze(IndexName:String):IHTTPResponse;
      function _forcemerge(IndexName:String):IHTTPResponse; overload;
      function _forcemerge:IHTTPResponse; overload;
      function _close(IndexName:String):IHTTPResponse;
      function _open(IndexName:String):IHTTPResponse;

      //template
      function CreateTemplate(template:String):IHTTPResponse;
      //task
      function _task: IHTTPResponse; overload;
      function _task(task_id: string): IHTTPResponse; overload;
      //search
      function _search(body:string): string; overload;
      function _search(body:string; index:string): string; overload;
      function _searchEX(reqest:string): IHTTPResponse;
       //send
      function _bulk(body:string; IndexName:String):IHTTPResponse;

      function _doc(body:string; IndexName:String):IHTTPResponse;
      // cluster
      function _cluster_health:IHTTPResponse;
      ///
      function GET(url:string):IHTTPResponse;
    private
      procedure OnRequestCompleted(const Sender: TObject; const AResponse: IHTTPResponse);
    end;

function Guid:string;
function LocalTimeToUTC(AValue: TDateTime): TDateTime;
function estimestamp :string;

implementation
const
    CR = #13#10;

function Guid:string;
var
  Uid: TGuid;
  GuidResult: HResult;
  s:string;
begin
result:='00000000-0000-0000-0000-000000000000';

GuidResult := CreateGuid(Uid);
if GuidResult = S_OK then
  begin
    s:=GuidToString(Uid);
    result:=Copy(s, 2, Length(s) - 2)
  end;
end;

function LocalTimeToUTC(AValue: TDateTime): TDateTime;
var
  ST1, ST2: TSystemTime;
  TZ: TTimeZoneInformation;
begin
  GetTimeZoneInformation(TZ);
  TZ.Bias := -TZ.Bias;
  TZ.StandardBias := -TZ.StandardBias;
  TZ.DaylightBias := -TZ.DaylightBias;

  DateTimeToSystemTime(AValue, ST1);

  SystemTimeToTzSpecificLocalTime(@TZ, ST1, ST2);

  Result := SystemTimeToDateTime(ST2);
end;

function estimestamp :string;
var
    us: string;
    dt: TDatetime;
    dtime: TDatetime;
    formatSettings : TFormatSettings;
begin
  dtime:= now;
  GetLocaleFormatSettings(MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US), formatSettings);
  dt := Frac(LocalTimeToUTC(dtime)); //fractional part of day
  dt := dt * 24*60*60; //number of seconds in that day
  us := IntToStr(Round(Frac(dt)*1000000));

  result:= Format('%sT%s.%s+0000', [FormatDateTime('yyyy-mm-dd',LocalTimeToUTC(dtime),formatSettings), FormatDateTime('hh:nn:ss',LocalTimeToUTC(dtime),formatSettings), us ]);
end;

procedure TElasticCLient.OnRequestCompleted(const Sender: TObject; const AResponse: IHTTPResponse);
begin
     Response := AResponse;
end;


constructor TElasticCLient.Create;
begin
  inherited Create;

  NetHTTPClient := TNetHTTPClient.Create(nil);
  NetHTTPClient.ContentType       :=  'application/json';
  NetHTTPClient.AcceptCharSet     :=  'UTF-8';
  NetHTTPClient.UserAgent         :=  'Delphi for ElasticSearch';
  NetHTTPClient.ConnectionTimeout := 1000;
  NetHTTPClient.ResponseTimeout   := 60000;

  NetHTTPClient.CustomHeaders['Connection']:= 'Keep-alive';

  HTTPRequest:= TNetHTTPRequest.Create(nil);
  version.major:=0;
end;

destructor TElasticCLient.Destroy;
begin
  NetHTTPClient.Free;
  inherited;
end;

function TElasticCLient.connected:boolean;
begin
  result:= (version.major > 0);
end;

function TElasticCLient.connect;
var
  JsonObject:TJSONObject;
  JSonValue:TJSonValue;
  response: IHTTPResponse;
begin
 NetHTTPClient.Asynchronous:= false;

 BaseUrl:= 'http://'+host+':'+port.ToString+'/';

 result:=false;

  try
    response:=NetHTTPClient.get(BaseURL);

    if response.StatusCode = 200 then
       begin
          JsonObject:=TJSONObject.Create;
          try                            //check Elastic Version
            JsonObject := TJSONObject.ParseJSONValue(response.ContentAsString(TEncoding.UTF8) ) as TJSONObject;
            if Assigned(JsonObject) then
               begin
                 if Assigned(JsonObject.GetValue('version')) then
                    begin
                       JSonValue:=TJSonObject.ParseJSONValue(JsonObject.GetValue('version').ToString);
                       try
                         version.numder := JSonValue.GetValue<string>('number');
                         version.major  := StrToInt(version.numder[1]); // rewrite, temporarily
                         result:=true;
                       finally
                         JSonValue.Free;
                       end;

                    end;

               end;
          finally
            JsonObject.Free;
          end;

       end;

       if Asynchronous then
           begin
              NetHTTPClient.Asynchronous:= true;
              NetHTTPClient.OnRequestCompleted  := OnRequestCompleted;
           end;


  except
     raise Exception.Create('connection error');
  end;


  //
end;


function TElasticCLient.GET(url:string):IHTTPResponse;
begin
  result:=NetHTTPClient.GET(url);
end;

function TElasticCLient._searchEX(reqest:string):IHTTPResponse;
var
  JsonToSend:TStringStream;
begin
   JsonToSend := TStringStream.Create;
   JsonToSend.WriteString(reqest);

   try
     result:= NetHTTPClient.Post(BaseURL, JsonToSend);
   finally
    JsonToSend.Free;
   end;

end;

////////////////////////////////////////////////////////////////////////////////

function TElasticCLient.indices:IHTTPResponse;
begin
   result:=NetHTTPClient.GET(BaseURL+'_cat/indices?format=JSON');
end;


function TElasticCLient.CreateTemplate(template:String):IHTTPResponse;
begin
  result:=NetHTTPClient.POST(BaseURL+'_template/rusiem?pretty',template);
end;

function TElasticCLient.CreateIndex(IndexName:String):IHTTPResponse;
begin
  result:=NetHTTPClient.Put(BaseURL+IndexName);
end;

function TElasticCLient.CreateIndex(IndexName:String; template:string):IHTTPResponse;
begin
  result:=NetHTTPClient.Put(BaseURL+IndexName, template);
end;

function TElasticCLient.DeleteIndex(IndexName:String):IHTTPResponse;
begin
   result:=NetHTTPClient.Delete(BaseURL+IndexName);
end;

function TElasticCLient._freeze(IndexName:String):IHTTPResponse;
begin
  parameters:=TStringStream.Create;
  try
    parameters.writestring('{}');
    parameters.Position := 0;
    result:=NetHTTPClient.POST(BaseURL+IndexName+'/_freeze',parameters);
  finally
   parameters.Free;
  end;
end;

function TElasticCLient._unfreeze(IndexName:String):IHTTPResponse;
begin
  parameters:=TStringStream.Create;
  try
    parameters.writestring('{}');
    parameters.Position := 0;
    result:=NetHTTPClient.POST(BaseURL+IndexName+'/_unfreeze',parameters);
  finally
   parameters.Free;
  end;
end;

function TElasticCLient._forcemerge(IndexName:String):IHTTPResponse;
begin
  parameters:=TStringStream.Create;
  try
    result:=NetHTTPClient.POST(BaseURL+IndexName+'/_forcemerge',parameters);
  finally
   parameters.Free;
  end;
end;

function TElasticCLient._forcemerge:IHTTPResponse;
begin
  parameters:=TStringStream.Create;
  try
    result:=NetHTTPClient.POST(BaseURL+'_forcemerge',parameters);
  finally
   parameters.Free;
  end;
end;


function TElasticCLient._open(IndexName:String):IHTTPResponse;
begin
  parameters:=TStringStream.Create;
  try
    result:=NetHTTPClient.POST(BaseURL+'_open',parameters);
  finally
   parameters.Free;
  end;
end;

function TElasticCLient._close(IndexName:String):IHTTPResponse;
begin
  parameters:=TStringStream.Create;
  try
    result:=NetHTTPClient.POST(BaseURL+'_close',parameters);
  finally
   parameters.Free;
  end;
end;


////////////////////////////////////////////////////////////////////////////////
function TElasticCLient._task: IHTTPResponse;
begin
   result:=NetHTTPClient.get(BaseURL+'_tasks');
end;


function TElasticCLient._task(task_id: string): IHTTPResponse;
begin
   result:=NetHTTPClient.GET(BaseURL+'_tasks/'+task_id);
end;
////////////////////////////////////////////////////////////////////////////////
function TElasticCLient._search(body:string): string;
begin
  if body<>'' then
      result:= NetHTTPClient.GET(BaseURL+'_search?q='+body).ContentAsString(TEncoding.UTF8)
  else
      result:= NetHTTPClient.GET(BaseURL+'_search').ContentAsString(TEncoding.UTF8);
end;

function TElasticCLient._search(body:string; index:string): string;
begin
  if body<>'' then
    result:= NetHTTPClient.GET(BaseURL+index+'/_search?q='+body).ContentAsString(TEncoding.UTF8)
  else
    result:= NetHTTPClient.GET(BaseURL+index+'/_search').ContentAsString(TEncoding.UTF8)
end;

////////////////////////////////////////////////////////////////////////////////
function TElasticCLient._doc(body:string; IndexName:String):IHTTPResponse;
var
  JsonToSend  : TStringStream;
begin
  IndexName:=LowerCase(IndexName);

  JsonToSend := TStringStream.Create;
  try
    JsonToSend.WriteString(body);
    JsonToSend.Position := 0;
    result:= NetHTTPClient.execute('post',BaseURL+IndexName+'/_doc/', JsonToSend);
  finally
    JsonToSend.Free;
  end;
end;


function TElasticCLient._bulk(body:string; IndexName:String):IHTTPResponse;
var
  Request     : TStringList;
  Index       : TJSONObject;
  IndexValue  : TJSONObject;
  JsonToSend  : TStringStream;
  i           : Integer;
begin
   IndexName:=LowerCase(IndexName); //fix invalid_index_name_exception. "Invalid index name must be lowercase"

   Request    := TStringList.Create;
   Index      := TJSONObject.Create;
   IndexValue := TJSONObject.Create;

   IndexValue.AddPair('_id', TJSONNull.Create);
   IndexValue.AddPair('_index', IndexName);

   if version.major < 7 then
     IndexValue.AddPair('_type', '_doc'); //ElasticSearch 5.6

   Index.AddPair('index', TJSONObject.ParseJSONValue(IndexValue.ToString) );

   JsonToSend := TStringStream.Create;

   try
      Request.Text:= body;

      for i := 0 to Request.Count-1 do
          begin
            JsonToSend.WriteString(Index.ToString+cr);
            JsonToSend.WriteString(Request.Strings[i]+cr);
          end;

      JsonToSend.Position:=0;
      //NetHTTPClient.Post(BaseURL+'_bulk', JsonToSend);
      result:= NetHTTPClient.Post(BaseURL+'_bulk', JsonToSend);
   finally
     Request.Free;
     FreeAndNil( Index );
     JsonToSend.Free;
   end;
end;

////////////////////////////////////////////////////////////////////////////////
function TElasticCLient.get_version:string;
begin
  result:= version.numder;
end;

////////////////////////////////// cluster /////////////////////////////////////
function TElasticCLient._cluster_health:IHTTPResponse;
begin
 result:= NetHTTPClient.GET(BaseURL+'_cluster/health');
end;



end.
