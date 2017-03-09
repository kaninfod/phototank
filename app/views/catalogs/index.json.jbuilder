json.catalogs @catalogs do |catalog|
  json.id           catalog.id
  json.name         catalog.name
  json.type         catalog.type
  json.url          catalog.photos.first.url('md')
end
