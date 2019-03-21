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
finalresult="[";
  for s = 1, #result do
    if(string.find(result[s], '"Symbol":"'..string.upper(symbol)..'"') ~= nil) then
      finalresult = finalresult..result[s]..',';
    end
  end
  return finalresult:sub(1, -2).."]";
end

