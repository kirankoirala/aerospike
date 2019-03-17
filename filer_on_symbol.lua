//THis will not work because, it takes whole bin and filters rows conaining given symbol.
//We need to give a list inside a record (blob) and filter only having the given symbol.


local function map_record(rec)
    local ret = map()
    for i, bin_name in ipairs(record.bin_names(rec)) do
      ret[bin_name] = rec[bin_name]
    end
    return ret
  end
  
  local function containing_symbol(bin, symbol)
    return function(rec)
      if rec[bin] then
        local s = rec[bin]
        if type(s) == 'string' then
          return string.find(s, symbol)
        end
      end
      return false
    end
  end
  
  function recs_containing_symbol(s, bin, symbol)
    return s : filter(containing_symbol(bin, symbol)) : map(map_record)
  end