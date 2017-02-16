class MasterCatalog < Catalog
  include RailsSettings::Extend
  validates :type, uniqueness: true
  def import
    return if self.settings.updating == true

    begin
      self.watch_path.each do |import_path|
        MasterImportSpawn.perform_later import_path, photo_id = nil, import_mode=self.import_mode
      end
      return true
    rescue Exception => e
      raise e
    end
  end

  def online
    true
  end

  def delete_photo(photo_id)
    pf = PhotoFilesApi::Api::new
    begin
      photo = self.photos.find(photo_id)

      pf.destroy photo.tm_id
      pf.destroy photo.md_id
      pf.destroy photo.lg_id
      pf.destroy photo.org_id

    rescue Exception => e
      Rails.logger.debug "Error: #{e}"
    end
  end

  def self.create_master()
    if MasterCatalog.count == 0
      c = Catalog.new
      c.type = "MasterCatalog"
      c.name = "Master Catalog"
      c.default = true
      c.path = ""
      c.watch_path = []
      c.save
      Rails.cache.delete("master_catalog")
    end
  end

end
