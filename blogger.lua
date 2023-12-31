local urlparse = require("socket.url")
local http = require("socket.http")
local cjson = require("cjson")
local utf8 = require("utf8")
local date = require("date")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_blog = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  item = percent_encode(item)
  if not target[item] then
---print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

find_path_loop = function(url, max_repetitions)
  local tested = {}
  local tempurl = urlparse.unescape(url)
  tempurl = string.match(tempurl, "^https?://[^/]+(.*)$")
  if not tempurl then
    return false
  end
  for s in string.gmatch(tempurl, "([^/%?&]+)") do
    s = string.lower(s)
    if not tested[s] then
      if s == "" then
        tested[s] = -2
      else
        tested[s] = 0
      end
    end
    tested[s] = tested[s] + 1
    if tested[s] == max_repetitions then
      return true
    end
  end
  return false
end

find_item = function(url)
  local value = string.match(url, "^https?://([^/]+)%.blogspot%.com/$")
  local type_ = "blog"
  local blog = nil
  if not value then
    blog, value = string.match(url, "^https?://([^/]+)%.blogspot%.com/([0-9][0-9][0-9][0-9]/[01][0-9]/.+%.html)$")
    type_ = "article"
  end
  if not value then
    blog, value = string.match(url, "^https?://([^/]+)%.blogspot%.com/(p/.+%.html)$")
    type_ = "page"
  end
  if not value then
    blog, value = string.match(url, "^https?://([^/]+)%.blogspot%.com/(search/label/[^%?&;]+)$")
    type_ = "search"
  end
  if not value then
    blog, value = string.match(url, "^https?://([^/]+)%.blogspot%.com/(search.*[%?&]updated%-max=.+)$")
    type_ = "search"
  end
  if not value then
    value = string.match(url, "^https?://(bp%.blogspot%.com/.+)$")
    type_ = "url"
  end
  if not value then
    value = string.match(url, "^https?://([^/]+%.bp%.blogspot%.com/.+)$")
    type_ = "url"
  end
  if not value then
    value = string.match(url, "^https?://([^/]+googleusercontent%.com/.+)$")
    type_ = "url"
  end
  if not value then
    value = string.match(url, "^https?://www%.blogger%.com/profile/([0-9]+)$")
    type_ = "profile"
  end
  if value then
    return {
      ["blog"]=blog,
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found and not ids[found["value"]] then
    item_type = found["type"]
    item_value = found["value"]
    item_blog = found["blog"]
    if item_blog then
      item_name_new = item_type .. ":" .. item_blog .. ":" .. item_value
    else
      item_name_new = item_type .. ":" .. item_value
    end
    if item_name_new ~= item_name then
      ids = {}
      ids[found["value"]] = true
      abortgrab = false
      initial_allowed = false
      tries = 0
      retry_url = false
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode = function(s)
  local result = ""
  for c in string.gmatch(s, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    result = result .. c
  end
  return result
end

allowed = function(url, parenturl)
  if ids[url] then
    return true
  end

  if not string.match(url, "^https?://[^/]")
    or string.match(url, "^https?://[^/]+/%*")
    or string.match(url, "^https?://[^/]+/b/stats%?")
    or string.match(url, "[\"']%s*%+%s*[a-zA-Z0-9_%[%]%-_%.]+%s*%+%s*[\"']")
    or string.match(url, "\\")
    or string.match(url, "%s")
    or string.match(url, "/////")
    or string.match(url, "%%url%%")
    or string.match(url, "%?m=0$")
    or string.match(url, "%.htmlfeeds/posts/")
    or string.match(url, "[%?&]showComment=")
    or string.match(url, "[%?&]widgetType=BlogArchive")
    or string.match(url, "[%?&]m%%3D1[%?&]m%%3D1")
    or string.match(url, "%?en%-[a-z][a-z]$")
    or string.match(url, "/search/label/.+searchsearchsearch$")
    or string.match(url, "/search/label/.*%?")
    or string.match(url, "/search.*/.*/.*/.*/")
    or (
      string.match(url, "/search/?%?q=")
      and not string.match(url, "updated%-max=")
    )
    or string.match(url, "/search.*[%?&]reverse%-paginate=")
    or string.match(url, "^https?://[^/]*blogger%.com/comment%.g%?")
    or string.match(url, "^https?://[^/]*blogger%.com/email%-post%.g%?")
    or string.match(url, "^https?://[^/]*blogger%.com/share%-post%.g%?")
    or string.match(url, "^https?://[^/]*blogger%.com/post%-edit%.g%?")
    or string.match(url, "^https?://[^/]*blogger%.com/navbar%.g%?")
    or string.match(url, "^https?://[^/]*blogger%.com/dyn%-css/authorization%.css%?")
    or (
      string.match(url, "^https?://[^/]*blogger%.com/feeds/[0-9]+/posts")
      and not string.match(url, "[%?&]dynamicviews=")
    )
    or (
      string.match(url, "^https?://[^/]*blogger%.com/feeds/[0-9]+/blogs")
      and not string.match(url, "[%?&]dynamicviews=")
    )
    or string.match(url, "^https?://[^/]*blogger%.com/rearrange%?")
    or string.match(url, "^https?://[^/]+/search/label/.*/ss?earch/")
    or string.match(url, "^https?://[^/]+/search/label/.*/label/")
    or (
      parenturl
      and not string.match(parenturl, "^https?://[^/]+/$")
      and not string.match(parenturl, "^https?://accounts%.google%.com/ServiceLogin")
      and string.match(parenturl, "^https?://(.+)$") ~= string.match(url, "^https?://(.+)$")
      and not string.match(url, "follower[iI][dD]")
      and string.match(url, "^https?://[^/]*blogger%.com/[^%.]+%.g%?")
    )
    or (
      parenturl
      and string.match(url, "^https?://[^/]*blogger%.com/feeds/")
      and string.match(parenturl, "[%?&]dynamicviews=")
    ) then
    return false
  end

  if parenturl
    and string.match(url, "/search.*[%?&]updated%-max=")
    and not string.match(parenturl, "^https?://[^/]+/$")
    and not string.match(parenturl, "^https?://[^/]+/search%?") then
    return false
  end

  if find_path_loop(url, 4) then
    return false
  end

  if string.match(url, "^https?://accounts%.google%.com/ServiceLogin%?passive=true&continue=") then
    return true
  end

  if item_type == "blog"
    and string.match(url, "^https://[^/]+%.blogspot%.com/feeds/")
    and not string.match(url, "[%?&]dynamicviews=") then
    return false
  end

  if select(2, string.gsub(url, "/", "")) < 3 then
    url = url .. "/"
  end

  local url_blog, url_tld = string.match(url, "^https?://([^/]+)%.blogspot%.([a-z][a-z][a-z]?%.?[a-z]?[a-z]?)/")
  if not url_blog then
    url_blog, url_tld = string.match(url, "^https?://([^/]+)%.blogger%.([a-z][a-z][a-z]?%.?[a-z]?[a-z]?)/")
  end
  local found = false
  for pattern, type_ in pairs({
    ["^https?://(bp%.blogspot%.com/.+)$"]="url",
    ["^https?://([^/]+%.bp%.blogspot%.com/.+)$"]="url",
    ["^https?://([^/]+googleusercontent%.com/.+)$"]="url",
    ["^https?://([^/%.]+)%.blogspot%.[a-z][a-z][a-z]?%.?[a-z]?[a-z]?/"]="blog",
    ["^https?://([^/%.]+)%.blogger%.[a-z][a-z][a-z]?%.?[a-z]?[a-z]?/"]="blog",
    ["^https?://[^/]+%.blogspot%.[a-z][a-z][a-z]?%.?[a-z]?[a-z]?/([0-9][0-9][0-9][0-9]/[01][0-9]/.+%.html)"]="article",
    ["^https?://[^/]+%.blogger%.[a-z][a-z][a-z]?%.?[a-z]?[a-z]?/([0-9][0-9][0-9][0-9]/[01][0-9]/.+%.html)"]="article",
    ["^https?://[^/]+%.blogspot%.[a-z][a-z][a-z]?%.?[a-z]?[a-z]?/(p/.+%.html)"]="page",
    ["^https?://[^/]+%.blogger%.[a-z][a-z][a-z]?%.?[a-z]?[a-z]?/(p/.+%.html)"]="page",
    ["^https?://[^/]+%.blogspot%.[a-z][a-z][a-z]?%.?[a-z]?[a-z]?/(search/label/[^%?&;]+)"]="search",
    ["^https?://[^/]+%.blogspot%.[a-z][a-z][a-z]?%.?[a-z]?[a-z]?/(search.*[%?&]updated%-max=.+)$"]="search",
    ["^https?://[^/]*blogger%.[a-z][a-z][a-z]?%.?[a-z]?[a-z]?/profile/([0-9]+)"]="profile",
    ["[%?&]follower[iI][dD]=([0-9]+)"]="profile"
  }) do
    local match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if type_ == "article" or type_ == "page" or type_ == "search" then
        new_item = type_ .. ":" .. url_blog .. ":" .. match
      end
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        if type_ ~= "blog"
          and not (url_blog and type_ ~= "url" and url_tld ~= "com") then
          found = true
        end
      end
    end
  end
  if found then
    return false
  end

  for _, pattern in pairs({
    "^https?://(.+)$",
    "([0-9]+)",
    "^https?://([^/]+)%.blogspot%.com/",
    "^https?://[^/]+%.blogspot%.com/(.+%.html)",
    "^https?://[^/]+%.blogspot%.com/(search/label/[^%?&;]+)",
    "^https?://[^/]+%.blogspot%.com/(search.*[%?&]updated%-max=.+)$"
  }) do
    local match = string.match(url, pattern)
    if match and ids[match] then
      return true
    end
  end

  if not string.match(url, "^https?://[^/]+%.blogspot%.com/")
    and not string.match(url, "^https?://[^/]+%.blogger%.com/") then
    discover_item(discovered_outlinks, url)
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[local https_url = string.gsub(url, "^http://", "https://")

  if allowed(url, parent["url"]) and not processed(url) and not processed(https_url) then
    addedtolist[url] = true
    return true
  end]]

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return utf8.char(tonumber(s, 16))
      end
    )
    newurl = string.gsub(
      newurl, "\\[xX]([0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return utf8.char(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function fix_case(newurl)
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^https?(://[^/]+/)(.*)$")
    return "https" .. string.lower(a) .. b
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    local temp_url = string.match(url_, "^(https?://[^/]+%.blogspot%.com/[0-9][0-9][0-9][0-9]/[0-9][0-9]/).")
    if temp_url then
      check(temp_url)
      check(string.match(temp_url, "(.+/)[0-9][0-9]/$"))
    end
    if not processed(url_)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  if allowed(url)
    and status_code < 300
    and item_type ~= "url" then
    html = read_file(file)
    --[[if found_new_post then
      io.stdout:write("Found new post, skipping blog for now.\n")
      io.stdout:flush()
      abort_item()
    end]]
    --[[if string.match(html, "<script src='[^']*blogblog%.com/dynamicviews/") then
      io.stdout:write("Found dynamicviews blog, skipping blog for now.\n")
      io.stdout:flush()
      abort_item()
    else]]if string.match(html, "[bB][lL][oO][gG][gG][eE][rR]%-video")
      or string.match(html, "blogger%.com/video") then
      io.stdout:write("Found a video, skipping item for now.\n")
      io.stdout:flush()
      abort_item()
    --[[elseif string.match(html, "'adultContent':%s*true")
      or string.match(html, "<meta[^>]+content='adult'") then
      io.stdout:write("Found an adult blog, skipping for now.\n")
      io.stdout:flush()
      abort_item()]]
    end
    if abortgrab then
      return urls
    end
    if string.match(url, "^https?://[^/]+%.blogspot%.com/$") then
      --[[for blog, year, month in string.gmatch(html, "https?://([^/]+)%.blogspot%.com/([0-9][0-9][0-9][0-9])/([01][0-9])/[a-z]") do
        year = tonumber(year)
        month = tonumber(month)
        if blog == item_value
          and month > 0 and month <= 12
          and year >= 2022 and year < 2024 then
          io.stdout:write("Found new post, skipping blog for now.\n")
          io.stdout:flush()
          abort_item()
          return {}
        end
      end]]
      if string.match(html, "<script src='[^']*blogblog%.com/dynamicviews/") then
        check(url .. "feeds/posts/default?alt=json&v=2&dynamicviews=1&orderby=published&max-results=25&rewriteforssl=true")
        check(url .. "feeds/pages/default?alt=json&v=2&dynamicviews=1&orderby=published&max-results=25&rewriteforssl=true")
        check(url .. "feeds/pages/default?alt=json&v=2&dynamicviews=1")
      end
      check(url .. "robots.txt")
      check(url .. "sitemap.xml")
      check(url .. "favicon.ico")
      check(url .. "atom.xml?redirect=false&max-results=")
      check(url .. "?m=1")
      local data = string.match(html, "_WidgetManager%._SetDataContext%((%[{.-}%])%);")
      if data then
        local blog_id = string.match(data, "'blogId'%s*:%s*'([0-9]+)'")
        ids[blog_id] = true
      end
      --[[for _, d in cjson.decode(data) do
        if d['name'] == 'blog' then
          ids[d['data']['blogId'] ] = true
        end
      end]]
    end
    if string.match(url, "/feeds/posts/default%?.*dynamicviews") then
      json = cjson.decode(html)
      if json["feed"]["entry"] then
        local latest_published = nil
        for _, data in pairs(json["feed"]["entry"]) do
          latest_published = data["published"]["$t"]
        end
        if latest_published then
          local published_max = date(latest_published)
          published_max:addseconds(-1)
          local newurl = string.gsub(url, "([%?&]published%-max=)[^&]+", "%1" .. published_max:fmt("%Y%%-%m%%-%dT%H%%%%3A%M%%%%3A%SZ"))
          if newurl == url then
            newurl = url .. "&published-max=" .. published_max:fmt("%Y-%m-%dT%H%3A%M%3A%SZ")
          end
          check(newurl)
        end
      end
    end
    if string.match(url, "dynamicviews") and string.match(html, "^{") then
      if not json then
        json = cjson.decode(html)
      end
      html = html .. " " .. flatten_json(json)
    end
    if item_type == "article"
      and string.match(url, "^https?://[^/]+/[0-9][0-9][0-9][0-9]/[0-9][0-9]/[^%.]+%.html$")
      and string.match(html, "<script src='[^']*blogblog%.com/dynamicviews/") then
      local post_id = string.match(html, '<link rel="alternate"[^>]+href="https?://[^"]+/feeds/([0-9]+)')
      ids[post_id] = true
      check(url .. "?dynamicviews=1&v=0")
      check(url .. "?m=1")
      check(urlparse.absolute(url, "/feeds/posts/default/" .. post_id .. "?alt=json&v=2&dynamicviews=1&rewriteforssl=true"))
      check(urlparse.absolute(url, "/feeds/" .. post_id .. "/comments/default?alt=json&v=2&dynamicviews=1&orderby=published&reverse=false&max-results=50&rewriteforssl=true"))
    end
    --[[if string.match(url, "^https?://[^/]+/robots.txt$")
      and not string.match(html, "Sitemap:%s+https?://[^/]+/sitemap%.xml") then
      error("Could not find sitemap in robots.txt.")
    end]]
    if (item_type == "article" or item_type == "search" or item_type == "page")
      and (
        string.match(html, "<div%s+[^>]*class='sidebar")
        or string.match(html, "<div%s+[^>]*id='sidebar")
        or string.match(html, "<div%s+[^>]*class='footer")
        or string.match(html, "<div%s+[^>]*id='footer")
        or string.match(html, "<div%s+[^>]*class='menu")
        or string.match(html, "<div%s+[^>]*class='topmenu")
        or string.match(html, "<div%s+[^>]*class='widget Label")
      ) then
      local separator = "\0\0"
      local count = 0
      local finding_end = false
      html = string.gsub(html, "(<%s*/?%s*div([^>]*)>)", function (s)
        if (
          string.match(s, "class='sidebar")
          or string.match(s, "id='sidebar")
          or string.match(s, "class='footer")
          or string.match(s, "id='footer")
          or string.match(s, "class='menu")
          or string.match(s, "class='topmenu")
          or string.match(s, "class='widget Label")
        ) and not finding_end then
          --print('Found start of a sidebar or footer.')
          count = count + 1
          finding_end = true
          return s .. separator
        elseif count > 0 then
          if string.match(s, "^<%s*/%s*div") then
            count = count - 1
          else
            count = count + 1
          end
          if count == 0 then
            --print('Found end of sidebar or footer.')
            done = true
            finding_end = false
            return separator .. s
          end
        end
        return s
      end)
      html = string.gsub(html, "%z%z.-%z%z", "")
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, 'href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  if abortgrab then
    urls = {}
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if http_stat["statcode"] >= 300
    and http_stat["statcode"] < 400
    and item_type == "blog"
    and string.match(url["url"], "^https?://[^/]+/$") then
    print("Found redirect. Skipping.")
    abort_item()
    return false
  end
  if http_stat["statcode"] == 302
    and not string.match(url["url"], "^https?://[^/]*blogger%.com/[^%.]+%.g%?")
    and not string.match(url["url"], "^https?://accounts%.google%.com/ServiceLogin%?passive=true&continue=") then
    retry_url = true
    return false
  end
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 302
    and http_stat["statcode"] ~= 400
    and http_stat["statcode"] ~= 404 then
    retry_url = true
    return false
  end
--[[  if string.match(url["url"], "^https?://[^/]+%.blogspot%.com/") then
    local html = read_file(http_stat["local_file"])
    if not string.match(html, "</html>") then
      print("Bad HTML.")
      retry_url = true
      return false
    end
  end]]
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if killgrab then
    return wget.actions.ABORT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code < 400 then
    downloaded[url["url"]] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  local sleep_time = 0

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    local maxtries = 2
    if item_type ~= "blog" then
      maxtries = 0
    end
    tries = tries + 1
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 10
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["urls-stash-blogger-ixcw2e3rotlitrgx"] = discovered_outlinks,
    ["blogger-wdpx1qiufiyqbv7s"] = discovered_items,
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      --print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 500 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


