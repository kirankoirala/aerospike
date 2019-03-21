function filter(r, symbol)
  result = {}

  binvalue = r['SubAccountBlob'];

  oneObj = ""
  
--return (binvalue);
x=1;
temp = "";
  for i = 1, #binvalue do
    oneChar  = string.sub(binvalue, i,i);
    if oneChar == "{" then
      oneObj="{";
    elseif oneChar == "}" then
      result[x] = oneObj.."}";
      temp = temp..'----'..oneObj.."}"
      x=x+1;
    else
      oneObj = oneObj..oneChar;
      oneChar="";
    end
  end
--return temp;
finalresult={};
  y=1;
  for s = 1, #result do
    if(string.find(result[s], '"Symbol":"'..string.upper(symbol)..'"') ~= nil) then
      finalresult[y]=result[s];
      y = y+1  ;
    end
  end
  return_result="[";
  for s = 1, #finalresult do
    return_result=return_result..finalresult[s]
  end
  return return_result.."]";
end

