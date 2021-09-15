unit libXmlToJson;
interface
uses
 xmldoc, 
 Xml.XMLIntf, 
 system.json, 
 system.Variants, 
 system.VarUtils, 
 sysutils, 
 dbxjson,
 System.Classes;
function xml_to_json(AXML : IXMLNode): string; overload;
procedure add_attributes_in_jsonObject(AXML : IXMLNode; AJSONTarget : TJSONObject);
implementation
procedure add_attributes_in_jsonObject(AXML : IXMLNode; AJSONTarget : TJSONObject);
var
  odata : OleVariant;
  j: integer;
begin
    if AXML.AttributeNodes.Count > 0 then
    begin
        for j := 0 to AXML.AttributeNodes.Count -1 do
        begin
            odata := AXML.AttributeNodes.Nodes[j].NodeValue;
            if  VarIsNull(odata) then
               odata := 'nil';
            AJSONTarget.AddPair('_' + AXML.AttributeNodes.Nodes[j].NodeName, odata);
        end;
    end;
end;
function xml_to_json(AXML : IXMLNode): string;
var
  oJS : TJSONObject;
  oJSListCollection : TJSONArray;
  i, j : integer;
  svalue : string;
  s: string;
begin
   s:=  AXML.xml;

    oJS := TJSONObject.Create;
    
    add_attributes_in_jsonObject(AXML, oJS);
    for I := 0 to AXML.ChildNodes.Count -1 do
    begin
        if AXML.ChildNodes.Nodes[i].Collection <> nil then
        begin
             oJSListCollection := TJSONArray.Create;
             try
                 for j := 0 to AXML.ChildNodes.Nodes[i].Collection.Count-1 do
                 begin
                    svalue := xml_to_json(AXML.ChildNodes.Nodes[i].Collection.Nodes[j]).replace('"{', '{').Replace('}"', '}');
                    oJSListCollection.Add(svalue);
                 end;
                 Result := oJSListCollection.ToJSON;
             finally
                 oJSListCollection.Free;
                 oJS.Free; 
             end;
             Exit;
        end
        else
        if (AXML.ChildNodes.Nodes[i].NodeType = ntElement) then
        begin
            if AXML.ChildNodes.Nodes[i].ChildNodes.Count = 1 then
            begin
                if (AXML.ChildNodes.Nodes[i].ChildNodes.First.NodeType = ntText) then
                begin
                   oJS.AddPair(AXML.ChildNodes.Nodes[i].NodeName, AXML.ChildNodes.Nodes[i].ChildNodes.First.NodeValue);
                end
                else if (AXML.ChildNodes.Nodes[i].ChildNodes.First.NodeType = ntCData) then
                begin
                   oJS.AddPair(AXML.ChildNodes.Nodes[i].NodeName, QuotedStr(AXML.ChildNodes.Nodes[i].ChildNodes.First.Text));
                end
                else 
                if (AXML.ChildNodes.Nodes[i].HasChildNodes) or (AXML.ChildNodes.Nodes[i].AttributeNodes.Count > 0) then
                    oJS.AddPair(AXML.ChildNodes.Nodes[i].NodeName, xml_to_json(AXML.ChildNodes.Nodes[i]))
                else
                    oJS.AddPair(AXML.ChildNodes.Nodes[i].NodeName, 'nil');
            end
            else 
            if (AXML.ChildNodes.Nodes[i].HasChildNodes) or (AXML.ChildNodes.Nodes[i].AttributeNodes.Count > 0) then
              begin
                oJS.AddPair(AXML.ChildNodes.Nodes[i].NodeName, xml_to_json(AXML.ChildNodes.Nodes[i]));
              end

            else
                oJS.AddPair(AXML.ChildNodes.Nodes[i].NodeName, 'nil');
        end
        else
        if (AXML.ChildNodes.Nodes[i].NodeType = ntCData) then
        begin
           oJS.AddPair(AXML.ChildNodes.Nodes[i].NodeName, AXML.ChildNodes.Nodes[i].NodeValue);
           // oJS.AddPair(AXML.ChildNodes.Nodes[i].NodeName, QuotedStr(AXML.ChildNodes.Nodes[i].ChildNodes.First.Text));

        end;
    end;
    Result := oJS.ToJSON.Replace('\', '').Replace('"[', '[').Replace(']"', ']').replace('"{', '{').Replace('}"', '}');
    oJS.Free;
end;
end.
