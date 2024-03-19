#!lua name=libFilter

local function bytesToInt(bytes)
    if #bytes ~= 4 then
        error("Invalid byte array size for date conversion")
    end
    return string.byte(bytes, 1) * 16777216 +
           string.byte(bytes, 2) * 65536 +
           string.byte(bytes, 3) * 256 +
           string.byte(bytes, 4)
end

local function extractDate(hashField)
    if #hashField < 4 then
        error("Invalid hashField size for date extraction")
    end
    return bytesToInt(string.sub(hashField, -4))
end

local function sessionBasedFilter_getUniqueFields(keys, args)

    if #keys ~= 1 then
        error("Invalid number of keys")
    end
    -- args must contain old and new session ids, threshold date and non-empty list of hashField-checksum pairs
    if #args <= 3 or (#args - 3) % 2 ~= 0 then
        error("Invalid number of args")
    end

    local hashName = keys[1]
    local oldSessionId = args[1]
    local newSessionId = args[2]
    local thresholdDate = bytesToInt(args[3])

    local hashFields = {}
    for i = 4, #args, 2 do
        table.insert(hashFields, oldSessionId..args[i])
    end
    local savedChecksums = redis.call("HMGET", hashName, unpack(hashFields))

    local result = {}
    local newChecksums = {}
    for i = 4, #args, 2 do
        local hashField = args[i]
        local checksum = args[i + 1]

        local j = (i - 2) / 2
        if checksum ~= savedChecksums[j] then
            table.insert(result, hashField)
        end

        if extractDate(hashField) > thresholdDate then
            table.insert(newChecksums, newSessionId..hashField)
            table.insert(newChecksums, checksum)
        end
    end

    redis.call("HDEL", hashName, unpack(hashFields))
    redis.call("HMSET", hashName, unpack(newChecksums))
    redis.call("EXPIRE", hashName, 120 * 24 * 60 * 60)

    return result
end

local function sessionBasedFilter_getNonArrivedFields(keys, args)

    if #keys ~= 1 then
        error("Invalid number of keys")
    end
    if #args ~= 7 then
        error("Invalid number of args")
    end

    local hashName = keys[1]
    local cursor = args[1]
    local oldSessionId = args[2]
    local newSessionId = args[3]
    local startDate = bytesToInt(args[4])
    local endDate = bytesToInt(args[5])
    local thresholdDate = bytesToInt(args[6])
    local batchSize = bytesToInt(args[7])

    local result = {}
    local deletingFields = {}
    local newChecksums = {}

    local scanResult = redis.call("HSCAN", hashName, cursor, "MATCH", oldSessionId.."*", "COUNT", batchSize)
    table.insert(result, scanResult[1]) -- next cursor
    scanResult = scanResult[2] -- key-value pairs

    for i = 1, #scanResult, 2 do
        local hashField = scanResult[i]
        local checksum = scanResult[i + 1]

        table.insert(deletingFields, hashField)

        local date = extractDate(hashField)
        if date >= startDate and date <= endDate then
            table.insert(result, string.sub(hashField,2)) -- remove session id
        elseif date > thresholdDate then
            table.insert(newChecksums, newSessionId..string.sub(hashField, 2))
            table.insert(newChecksums, checksum)
        end
    end

    if #deletingFields > 0 then
        redis.call("HDEL", hashName, unpack(deletingFields))
    end
    if #newChecksums > 0 then
        redis.call("HMSET", hashName, unpack(newChecksums))
    end

    return result
end

redis.register_function('sessionBasedFilter_getUniqueFields', sessionBasedFilter_getUniqueFields)
redis.register_function('sessionBasedFilter_getNonArrivedFields', sessionBasedFilter_getNonArrivedFields)
